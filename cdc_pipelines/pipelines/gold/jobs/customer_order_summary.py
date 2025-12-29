"""Customer order summary aggregation processor."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, countDistinct,
    when, date_format, dayofweek, datediff, current_date,
    row_number, lit, round as spark_round, max as spark_max, min as spark_min
)
from pyspark.sql.window import Window

from cdc_pipelines.common.logger import get_logger
from cdc_pipelines.pipelines.gold.silver_reader import SilverReader
from cdc_pipelines.pipelines.gold.gold_writer import GoldWriter

logger = get_logger(__name__)


def build_customer_order_summary(spark, config: dict):
    """
    Build customer order summary table.
    Aggregates customer behavior metrics.

    Args:
        spark: SparkSession instance
        config: Pipeline configuration dictionary

    Returns:
        None - writes directly to Delta table
    """
    logger.info("Building customer_order_summary table")
    
    silver_reader = SilverReader(spark)
    
    # Read silver tables
    silver_path = config['delta']['silver_path']
    
    orders_df = silver_reader.read_fact_table(
        f"{silver_path}/orders", "orders"
    )
    order_products_df = silver_reader.read_fact_table(
        f"{silver_path}/order_products", "order_products"
    )
    
    if orders_df is None or order_products_df is None:
        logger.warning("Required tables not available, skipping customer_order_summary")
        return
    
    # Calculate products per order
    products_per_order = order_products_df.groupBy("order_id").agg(
        spark_sum(lit(1)).alias("products_in_order"),
        spark_sum(when(col("reordered") == 1, 1).otherwise(0)).alias("reordered_in_order")
    )
    
    # Join with orders
    orders_enriched = orders_df.join(products_per_order, "order_id", "left")
    
    # Calculate day of week from order_date
    orders_enriched = orders_enriched.withColumn(
        "order_dow", dayofweek(col("order_date"))
    )
    
    # Aggregate by customer
    customer_metrics = orders_enriched.groupBy("user_id").agg(
        count("order_id").alias("total_orders"),
        spark_sum("products_in_order").alias("total_products"),
        avg("products_in_order").alias("avg_products_per_order"),
        avg("days_since_prior_order").alias("avg_days_between_orders"),
        spark_min("order_date").alias("first_order_date"),
        spark_max("order_date").alias("last_order_date"),
        spark_sum("reordered_in_order").alias("total_reordered_products")
    )
    
    # Calculate most common order patterns
    dow_window = Window.partitionBy("user_id").orderBy(col("dow_count").desc())
    hour_window = Window.partitionBy("user_id").orderBy(col("hour_count").desc())
    
    # Most common day of week
    dow_counts = orders_enriched.groupBy("user_id", "order_dow").agg(
        count("*").alias("dow_count")
    )
    most_common_dow = dow_counts.withColumn("row_num", row_number().over(dow_window)) \
        .filter(col("row_num") == 1) \
        .select("user_id", col("order_dow").alias("most_common_order_dow"))
    
    # Most common hour
    hour_counts = orders_enriched.groupBy("user_id", "order_hour_of_day").agg(
        count("*").alias("hour_count")
    )
    most_common_hour = hour_counts.withColumn("row_num", row_number().over(hour_window)) \
        .filter(col("row_num") == 1) \
        .select("user_id", col("order_hour_of_day").alias("most_common_order_hour"))
    
    # Average hour
    avg_hour = orders_enriched.groupBy("user_id").agg(
        avg("order_hour_of_day").alias("avg_order_hour")
    )
    
    # Join all metrics
    result = customer_metrics \
        .join(most_common_dow, "user_id", "left") \
        .join(most_common_hour, "user_id", "left") \
        .join(avg_hour, "user_id", "left")
    
    # Calculate derived metrics
    result = result.withColumn(
        "days_since_last_order",
        datediff(current_date(), col("last_order_date")).cast("integer")
    ).withColumn(
        "customer_lifetime_days",
        datediff(col("last_order_date"), col("first_order_date")).cast("integer")
    ).withColumn(
        "reorder_rate",
        spark_round(col("total_reordered_products") / col("total_products"), 4)
    ).withColumn(
        "is_active",
        when(col("days_since_last_order") <= 7, True).otherwise(False)
    ).withColumn(
        "customer_segment",
        when(col("total_orders") >= 20, "high")
        .when(col("total_orders") >= 10, "medium")
        .otherwise("low")
    )
    
    # Select final columns
    result = result.select(
        "user_id",
        "total_orders",
        "total_products",
        "avg_products_per_order",
        "avg_days_between_orders",
        "most_common_order_dow",
        "most_common_order_hour",
        "avg_order_hour",
        "first_order_date",
        "last_order_date",
        "days_since_last_order",
        "customer_lifetime_days",
        "total_reordered_products",
        "reorder_rate",
        "is_active",
        "customer_segment"
    )
    
    # Write to gold table
    gold_path = f"{config['delta']['gold_path']}/customer_order_summary"
    writer = GoldWriter(spark, "customer_order_summary", gold_path)
    writer.write_complete(result)
    
    logger.info("Completed customer_order_summary table")
