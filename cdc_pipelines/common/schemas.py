"""Schema definitions for CDC and Delta tables."""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
    BooleanType,
    DecimalType,
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


# Kafka Message Schema (with key and value)
KAFKA_SCHEMA = StructType(
    [
        StructField("key", StringType(), True),
        StructField("value", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("timestampType", IntegerType(), True),
    ]
)


# Bronze Layer Metadata Schema
BRONZE_METADATA_SCHEMA = StructType(
    [
        StructField("kafka_topic", StringType(), False),
        StructField("kafka_partition", IntegerType(), False),
        StructField("kafka_offset", LongType(), False),
        StructField("kafka_timestamp", TimestampType(), False),
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("cdc_operation", StringType(), False),
        StructField("cdc_timestamp", TimestampType(), True),
    ]
)


# Silver Layer SCD2 Schema
SCD2_SCHEMA = StructType(
    [
        StructField("valid_from", TimestampType(), False),
        StructField("valid_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
        StructField("row_hash", StringType(), False),
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
        StructField("order_dow", IntegerType(), True),
        StructField("order_hour_of_day", IntegerType(), True),
        StructField("days_since_prior_order", DecimalType(10, 2), True),
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
        # SCD Type 2 Metadata
        StructField("effective_from", TimestampType(), False),
        StructField("effective_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
        StructField("version", IntegerType(), False),
        # Lineage Tracking
        StructField("created_from_bronze_ts", TimestampType(), False),
        StructField("updated_from_bronze_ts", TimestampType(), False),
        StructField("source_operation", StringType(), False),
        StructField("data_quality_score", IntegerType(), True),
    ]
)

SILVER_AISLES_SCHEMA = StructType(
    [
        # Business Keys
        StructField("aisle_id", IntegerType(), False),
        StructField("aisle_sk", LongType(), False),
        # Attributes
        StructField("aisle", StringType(), True),
        # SCD Type 2 Metadata
        StructField("effective_from", TimestampType(), False),
        StructField("effective_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
        StructField("version", IntegerType(), False),
        # Lineage Tracking
        StructField("created_from_bronze_ts", TimestampType(), False),
        StructField("updated_from_bronze_ts", TimestampType(), False),
        StructField("source_operation", StringType(), False),
        StructField("data_quality_score", IntegerType(), True),
    ]
)

SILVER_DEPARTMENTS_SCHEMA = StructType(
    [
        # Business Keys
        StructField("department_id", IntegerType(), False),
        StructField("department_sk", LongType(), False),
        # Attributes
        StructField("department", StringType(), True),
        # SCD Type 2 Metadata
        StructField("effective_from", TimestampType(), False),
        StructField("effective_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
        StructField("version", IntegerType(), False),
        # Lineage Tracking
        StructField("created_from_bronze_ts", TimestampType(), False),
        StructField("updated_from_bronze_ts", TimestampType(), False),
        StructField("source_operation", StringType(), False),
        StructField("data_quality_score", IntegerType(), True),
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
        StructField("order_dow", IntegerType(), True),
        StructField("order_hour_of_day", IntegerType(), True),
        StructField("days_since_prior_order", DecimalType(10, 2), True),
        # Change Tracking
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
        StructField("is_deleted", BooleanType(), False),
        StructField("deleted_at", TimestampType(), True),
        # Lineage
        StructField("first_seen_bronze_ts", TimestampType(), False),
        StructField("last_updated_bronze_ts", TimestampType(), False),
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
        # Change Tracking
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
        StructField("is_deleted", BooleanType(), False),
        StructField("deleted_at", TimestampType(), True),
        # Lineage
        StructField("first_seen_bronze_ts", TimestampType(), False),
        StructField("last_updated_bronze_ts", TimestampType(), False),
        StructField("update_count", IntegerType(), False),
    ]
)


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


def add_bronze_metadata(schema: StructType) -> StructType:
    """
    Add bronze metadata columns to a schema.

    Args:
        schema: Original schema

    Returns:
        Schema with metadata columns added
    """
    return StructType(list(schema.fields) + list(BRONZE_METADATA_SCHEMA.fields))


def add_scd2_columns(schema: StructType) -> StructType:
    """
    Add SCD2 columns to a schema.

    Args:
        schema: Original schema

    Returns:
        Schema with SCD2 columns added
    """
    return StructType(list(schema.fields) + list(SCD2_SCHEMA.fields))
