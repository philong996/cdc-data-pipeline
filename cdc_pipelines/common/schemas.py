"""Schema definitions for CDC and Delta tables."""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
    BooleanType,
)


# Debezium CDC Envelope Schema
DEBEZIUM_SCHEMA = StructType(
    [
        StructField(
            "before",
            StructType([StructField("payload", StringType(), True)]),
            True,
        ),
        StructField(
            "after",
            StructType([StructField("payload", StringType(), True)]),
            True,
        ),
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
