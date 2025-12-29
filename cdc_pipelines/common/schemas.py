"""Schema definitions for CDC and Delta tables."""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    IntegerType,
    LongType,
    TimestampType,
    BooleanType,
    DecimalType,
    DoubleType
)


# Debezium CDC Envelope Schema
# Full Debezium message structure with schema and payload fields.
# Note: before and after are StringType to keep the schema general across all tables.
# They will be parsed with table-specific schemas in the processing pipeline.
DEBEZIUM_SCHEMA = StructType(
    [
        StructField("schema", StringType(), True),  # Schema definition (not parsed)
        StructField(
            "payload",
            StructType(
                [
                    StructField("before", StringType(), True),
                    StructField("after", StringType(), True),
                    StructField(
                        "source",
                        StructType(
                            [
                                StructField("version", StringType(), True),
                                StructField("connector", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("ts_ms", LongType(), True),
                                StructField("snapshot", StringType(), True),
                                StructField("db", StringType(), True),
                                StructField("sequence", StringType(), True),
                                StructField("schema", StringType(), True),
                                StructField("table", StringType(), True),
                                StructField("txId", LongType(), True),
                                StructField("lsn", LongType(), True),
                                StructField("xmin", LongType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField("op", StringType(), True),
                    StructField("ts_ms", LongType(), True),
                    StructField(
                        "transaction",
                        StructType(
                            [
                                StructField("id", StringType(), True),
                                StructField("total_order", LongType(), True),
                                StructField("data_collection_order", LongType(), True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)


# Source table schemas (from PostgreSQL)
PRODUCTS_SOURCE_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), True),
        StructField("aisle_id", IntegerType(), True),
        StructField("department_id", IntegerType(), True),
    ]
)

AISLES_SOURCE_SCHEMA = StructType(
    [
        StructField("aisle_id", IntegerType(), False),
        StructField("aisle", StringType(), True),
    ]
)

DEPARTMENTS_SOURCE_SCHEMA = StructType(
    [
        StructField("department_id", IntegerType(), False),
        StructField("department", StringType(), True),
    ]
)

ORDERS_SOURCE_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("user_id", IntegerType(), True),
        StructField("order_number", IntegerType(), True),
        StructField("order_date", IntegerType(), True),  # Debezium sends as days since epoch
        StructField("order_hour_of_day", IntegerType(), True),
        StructField("days_since_prior_order", IntegerType(), True),
    ]
)

ORDER_PRODUCTS_SOURCE_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("add_to_cart_order", IntegerType(), True),
        StructField("reordered", IntegerType(), True),
    ]
)


# Silver Layer Dimension Table Schema (SCD Type 2)
# For products, aisles, departments
SILVER_PRODUCTS_SCHEMA = StructType(
    [
        # Business Keys
        StructField("product_id", IntegerType(), False),
        StructField("product_sk", LongType(), False),
        # Attributes
        StructField("product_name", StringType(), True),
        StructField("aisle_id", IntegerType(), True),
        StructField("department_id", IntegerType(), True),        
        # CDC Metadata
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", LongType(), False),
        StructField("kafka_timestamp", TimestampType(), False),
        StructField("cdc_operation", StringType(), False),
        StructField("cdc_timestamp", TimestampType(), True),
        # SCD Type 2 Metadata
        StructField("effective_from", TimestampType(), False),
        StructField("effective_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
        StructField("version", IntegerType(), False),
        # Lineage Tracking
        StructField("created_from_bronze_ts", TimestampType(), False),
        StructField("source_operation", StringType(), False),
    ]
)

SILVER_AISLES_SCHEMA = StructType(
    [
        # Business Keys
        StructField("aisle_id", IntegerType(), False),
        StructField("aisle_sk", LongType(), False),
        # Attributes
        StructField("aisle", StringType(), True),
        # CDC Metadata
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", LongType(), False),
        StructField("kafka_timestamp", TimestampType(), False),
        StructField("cdc_operation", StringType(), False),
        StructField("cdc_timestamp", TimestampType(), True),
        # SCD Type 2 Metadata
        StructField("effective_from", TimestampType(), False),
        StructField("effective_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
        StructField("version", IntegerType(), False),
        # Lineage Tracking
        StructField("created_from_bronze_ts", TimestampType(), False),
        StructField("source_operation", StringType(), False),
    ]
)

SILVER_DEPARTMENTS_SCHEMA = StructType(
    [
        # Business Keys
        StructField("department_id", IntegerType(), False),
        StructField("department_sk", LongType(), False),
        # Attributes
        StructField("department", StringType(), True),
        # CDC Metadata
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", LongType(), False),
        StructField("kafka_timestamp", TimestampType(), False),
        StructField("cdc_operation", StringType(), False),
        StructField("cdc_timestamp", TimestampType(), True),
        # SCD Type 2 Metadata
        StructField("effective_from", TimestampType(), False),
        StructField("effective_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
        StructField("version", IntegerType(), False),
        # Lineage Tracking
        StructField("created_from_bronze_ts", TimestampType(), False),
        StructField("source_operation", StringType(), False),
    ]
)


# Silver Layer Fact Table Schema (Append-Only with Soft Deletes)
# For orders, order_products
SILVER_ORDERS_SCHEMA = StructType(
    [
        # Primary Key
        StructField("order_id", IntegerType(), False),
        # Attributes
        StructField("user_id", IntegerType(), True),
        StructField("order_number", IntegerType(), True),
        StructField("order_date", DateType(), True),
        StructField("order_hour_of_day", IntegerType(), True),
        StructField("days_since_prior_order", DecimalType(10, 2), True),
        # CDC Metadata
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", LongType(), False),
        StructField("kafka_timestamp", TimestampType(), False),
        StructField("cdc_operation", StringType(), False),
        StructField("cdc_timestamp", TimestampType(), True),
        # Change Tracking
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
        StructField("is_deleted", BooleanType(), False),
        StructField("deleted_at", TimestampType(), True),
        # Lineage
        StructField("first_seen_bronze_ts", TimestampType(), False),
        StructField("update_count", IntegerType(), False),
    ]
)

SILVER_ORDER_PRODUCTS_SCHEMA = StructType(
    [
        # Composite Primary Key
        StructField("order_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        # Attributes
        StructField("add_to_cart_order", IntegerType(), True),
        StructField("reordered", IntegerType(), True),
        # CDC Metadata
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", LongType(), False),
        StructField("kafka_timestamp", TimestampType(), False),
        StructField("cdc_operation", StringType(), False),
        StructField("cdc_timestamp", TimestampType(), True),
        # Change Tracking
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
        StructField("is_deleted", BooleanType(), False),
        StructField("deleted_at", TimestampType(), True),
        # Lineage
        StructField("first_seen_bronze_ts", TimestampType(), False),
        StructField("update_count", IntegerType(), False),
    ]
)

GOLD_PRODUCT_PERFORMANCE_DAILY_SCHEMA = StructType([
    # Dimensions (Current SCD Type 2 attributes)
    StructField("date", DateType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("product_sk", LongType(), False),  # Join to current version
    StructField("product_name", StringType(), True),
    StructField("aisle_id", IntegerType(), True),
    StructField("aisle", StringType(), True),
    StructField("department_id", IntegerType(), True),
    StructField("department", StringType(), True),
    
    # Metrics
    StructField("total_orders", LongType(), False),
    StructField("total_quantity", LongType(), False),
    StructField("unique_customers", LongType(), False),
    StructField("reorder_count", LongType(), False),
    StructField("reorder_rate", DoubleType(), True),
    StructField("avg_cart_position", DoubleType(), True),
    
    # Metadata
    StructField("last_updated_at", TimestampType(), False),
])

GOLD_CUSTOMER_ORDER_SUMMARY_SCHEMA = StructType([
    # Customer Identity
    StructField("user_id", IntegerType(), False),
    
    # Order Behavior
    StructField("total_orders", IntegerType(), False),
    StructField("total_products", LongType(), False),
    StructField("avg_products_per_order", DoubleType(), True),
    StructField("avg_days_between_orders", DoubleType(), True),
    
    # Timing Patterns
    StructField("most_common_order_dow", IntegerType(), True),
    StructField("most_common_order_hour", IntegerType(), True),
    StructField("avg_order_hour", DoubleType(), True),
    
    # Recency Metrics
    StructField("first_order_date", DateType(), True),
    StructField("last_order_date", DateType(), True),
    StructField("days_since_last_order", IntegerType(), True),
    StructField("customer_lifetime_days", IntegerType(), True),
    
    # Reorder Behavior
    StructField("total_reordered_products", LongType(), False),
    StructField("reorder_rate", DoubleType(), True),
    
    # Customer Status
    StructField("is_active", BooleanType(), False),  # Ordered in last 7 days
    StructField("customer_segment", StringType(), True),  # high/medium/low frequency
    
    # Metadata
    StructField("last_updated_at", TimestampType(), False),
])

GOLD_ORDER_HOURLY_PATTERNS_SCHEMA = StructType([
    # Time Dimensions
    StructField("date", DateType(), False),
    StructField("hour_of_day", IntegerType(), False),
    StructField("day_of_week", IntegerType(), False),
    StructField("day_name", StringType(), True),
    StructField("is_weekend", BooleanType(), False),
    
    # Order Metrics
    StructField("total_orders", LongType(), False),
    StructField("unique_customers", LongType(), False),
    StructField("avg_products_per_order", DoubleType(), True),
    StructField("total_products", LongType(), False),
    
    # Metadata
    StructField("last_updated_at", TimestampType(), False),
])


# Schema mapping for easy lookup
SOURCE_SCHEMAS = {
    "products": PRODUCTS_SOURCE_SCHEMA,
    "aisles": AISLES_SOURCE_SCHEMA,
    "departments": DEPARTMENTS_SOURCE_SCHEMA,
    "orders": ORDERS_SOURCE_SCHEMA,
    "order_products": ORDER_PRODUCTS_SOURCE_SCHEMA,
}

SILVER_SCHEMAS = {
    "products": SILVER_PRODUCTS_SCHEMA,
    "aisles": SILVER_AISLES_SCHEMA,
    "departments": SILVER_DEPARTMENTS_SCHEMA,
    "orders": SILVER_ORDERS_SCHEMA,
    "order_products": SILVER_ORDER_PRODUCTS_SCHEMA,
}


def get_table_schema(table_name: str, schema_dict: dict) -> StructType:
    """
    Convert schema dictionary to Spark StructType.

    Args:
        table_name: Name of the table
        schema_dict: Schema dictionary from tables.yaml

    Returns:
        StructType representing the schema
    """
    type_mapping = {
        "integer": IntegerType(),
        "long": LongType(),
        "string": StringType(),
        "timestamp": TimestampType(),
        "boolean": BooleanType(),
    }

    fields = []
    for field_name, field_type in schema_dict.items():
        # Handle decimal types
        if field_type.startswith("decimal"):
            spark_type = StringType()  # Simplified; use DecimalType for production
        else:
            spark_type = type_mapping.get(field_type, StringType())

        fields.append(StructField(field_name, spark_type, True))

    return StructType(fields)



