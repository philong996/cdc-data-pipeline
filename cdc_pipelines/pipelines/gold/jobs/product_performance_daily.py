"""Product performance daily aggregation processor."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, countDistinct,
    when, lit, round as spark_round
)

from cdc_pipelines.common.logger import get_logger
from cdc_pipelines.pipelines.gold.silver_reader import SilverReader
from cdc_pipelines.pipelines.gold.gold_writer import GoldWriter

logger = get_logger(__name__)


def build_product_performance_daily(spark, config: dict):
    """
    Build daily product performance analytics table.
    Aggregates product sales metrics by date.

    Args:
        spark: SparkSession instance
        config: Pipeline configuration dictionary

    Returns:
        None - writes directly to Delta table
    """
    logger.info("Building product_performance_daily table")
    
    silver_reader = SilverReader(spark)
    
    # Read silver tables
    silver_path = config['delta']['silver_path']
    
    products_df = silver_reader.read_current_dimension(
        f"{silver_path}/products", "products"
    )
    aisles_df = silver_reader.read_current_dimension(
        f"{silver_path}/aisles", "aisles"
    )
    departments_df = silver_reader.read_current_dimension(
        f"{silver_path}/departments", "departments"
    )
    orders_df = silver_reader.read_fact_table(
        f"{silver_path}/orders", "orders"
    )
    order_products_df = silver_reader.read_fact_table(
        f"{silver_path}/order_products", "order_products"
    )
    
    if any(df is None for df in [products_df, orders_df, order_products_df]):
        logger.warning("Required tables not available, skipping product_performance_daily")
        return
    
    # Join order products with orders to get date
    order_with_date = order_products_df.join(
        orders_df.select("order_id", "order_date", "user_id"),
        "order_id"
    )
    
    # Aggregate by date and product
    daily_metrics = order_with_date.groupBy("order_date", "product_id").agg(
        count("order_id").alias("total_orders"),
        spark_sum(lit(1)).alias("total_quantity"),
        countDistinct("user_id").alias("unique_customers"),
        spark_sum(when(col("reordered") == 1, 1).otherwise(0)).alias("reorder_count"),
        avg("add_to_cart_order").alias("avg_cart_position")
    )
    
    # Calculate reorder rate
    daily_metrics = daily_metrics.withColumn(
        "reorder_rate",
        spark_round(col("reorder_count") / col("total_quantity"), 4)
    )
    
    # Join with current product dimensions
    result = daily_metrics \
        .join(products_df.select(
            "product_id", "product_sk", "product_name", 
            "aisle_id", "department_id"
        ), "product_id")
    
    # Join with aisles and departments if available
    if aisles_df is not None:
        result = result.join(
            aisles_df.select("aisle_id", col("aisle")),
            "aisle_id",
            "left"
        )
    else:
        result = result.withColumn("aisle", lit(None).cast("string"))
    
    if departments_df is not None:
        result = result.join(
            departments_df.select("department_id", col("department")),
            "department_id",
            "left"
        )
    else:
        result = result.withColumn("department", lit(None).cast("string"))
    
    # Select final columns in schema order
    result = result.select(
        col("order_date").alias("date"),
        "product_id",
        "product_sk",
        "product_name",
        "aisle_id",
        "aisle",
        "department_id",
        "department",
        "total_orders",
        "total_quantity",
        "unique_customers",
        "reorder_count",
        "reorder_rate",
        "avg_cart_position"
    )
    
    # Write to gold table
    gold_path = f"{config['delta']['gold_path']}/product_performance_daily"
    writer = GoldWriter(spark, "product_performance_daily", gold_path)
    writer.write_complete(result)
    
    logger.info("Completed product_performance_daily table")
