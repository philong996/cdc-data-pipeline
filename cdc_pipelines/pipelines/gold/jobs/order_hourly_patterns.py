"""Order hourly patterns aggregation processor."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, countDistinct,
    when, date_format, dayofweek, lit
)

from cdc_pipelines.common.logger import get_logger
from cdc_pipelines.pipelines.gold.silver_reader import SilverReader
from cdc_pipelines.pipelines.gold.gold_writer import GoldWriter

logger = get_logger(__name__)


def build_order_hourly_patterns(spark, config: dict):
    """
    Build order hourly patterns table.
    Aggregates order metrics by date and hour.

    Args:
        spark: SparkSession instance
        config: Pipeline configuration dictionary

    Returns:
        None - writes directly to Delta table
    """
    logger.info("Building order_hourly_patterns table")
    
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
        logger.warning("Required tables not available, skipping order_hourly_patterns")
        return
    
    # Calculate products per order
    products_per_order = order_products_df.groupBy("order_id").agg(
        spark_sum(lit(1)).alias("products_in_order")
    )
    
    # Join with orders
    orders_enriched = orders_df.join(products_per_order, "order_id", "left")
    
    # Add time dimensions
    orders_enriched = orders_enriched.withColumn(
        "day_of_week", dayofweek(col("order_date"))
    ).withColumn(
        "day_name", date_format(col("order_date"), "EEEE")
    ).withColumn(
        "is_weekend",
        when(col("day_of_week").isin([1, 7]), True).otherwise(False)
    )
    
    # Aggregate by date and hour
    result = orders_enriched.groupBy(
        "order_date",
        "order_hour_of_day",
        "day_of_week",
        "day_name",
        "is_weekend"
    ).agg(
        count("order_id").alias("total_orders"),
        countDistinct("user_id").alias("unique_customers"),
        avg("products_in_order").alias("avg_products_per_order"),
        spark_sum("products_in_order").alias("total_products")
    )
    
    # Select final columns
    result = result.select(
        col("order_date").alias("date"),
        col("order_hour_of_day").alias("hour_of_day"),
        "day_of_week",
        "day_name",
        "is_weekend",
        "total_orders",
        "unique_customers",
        "avg_products_per_order",
        "total_products"
    )
    
    # Write to gold table
    gold_path = f"{config['delta']['gold_path']}/order_hourly_patterns"
    writer = GoldWriter(spark, "order_hourly_patterns", gold_path)
    writer.write_complete(result)
    
    logger.info("Completed order_hourly_patterns table")
