"""CDC parser for Debezium envelope format."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    get_json_object,
    current_timestamp,
    lit,
    when,
)
from cdc_pipelines.common.logger import get_logger
from cdc_pipelines.common.constants import CDC_OPERATIONS

logger = get_logger(__name__)


class CDCParser:
    """Parses Debezium CDC envelope format."""

    def __init__(self):
        """Initialize CDC parser."""
        pass

    def parse_cdc_envelope(self, df: DataFrame) -> DataFrame:
        """
        Parse Debezium CDC envelope from Kafka message.

        Args:
            df: Raw Kafka DataFrame

        Returns:
            Parsed DataFrame with CDC fields
        """
        logger.info("Parsing CDC envelope from Kafka messages")

        # Cast value to string
        df = df.withColumn("value_str", col("value").cast("string"))

        # Extract CDC operation
        df = df.withColumn(
            "cdc_operation",
            get_json_object(col("value_str"), "$.payload.op")
        )

        # Extract before state
        df = df.withColumn(
            "before",
            get_json_object(col("value_str"), "$.payload.before")
        )

        # Extract after state
        df = df.withColumn(
            "after",
            get_json_object(col("value_str"), "$.payload.after")
        )

        # Extract source metadata
        df = df.withColumn(
            "source_db",
            get_json_object(col("value_str"), "$.payload.source.db")
        )
        df = df.withColumn(
            "source_table",
            get_json_object(col("value_str"), "$.payload.source.table")
        )
        df = df.withColumn(
            "source_ts_ms",
            get_json_object(col("value_str"), "$.payload.source.ts_ms")
        )

        # Extract transaction info
        df = df.withColumn(
            "transaction_id",
            get_json_object(col("value_str"), "$.payload.transaction.id")
        )

        logger.info("CDC envelope parsed successfully")

        return df

    def extract_payload(self, df: DataFrame, operation_filter: str = None) -> DataFrame:
        """
        Extract the actual data payload from CDC envelope.

        Args:
            df: DataFrame with parsed CDC envelope
            operation_filter: Filter by CDC operation (c, r, u, d)

        Returns:
            DataFrame with extracted payload
        """
        logger.info("Extracting payload from CDC envelope")

        # Filter by operation if specified
        if operation_filter:
            df = df.filter(col("cdc_operation") == operation_filter)

        # For creates and updates, use 'after'
        # For deletes, use 'before'
        # For reads, use 'after'
        df = df.withColumn(
            "payload",
            col("after")
        )

        # For delete operations, use 'before' instead
        df = df.withColumn(
            "payload",
            col("before")
        )

        return df

    def map_operation_type(self, df: DataFrame) -> DataFrame:
        """
        Map CDC operation codes to readable names.

        Args:
            df: DataFrame with cdc_operation column

        Returns:
            DataFrame with mapped operation names
        """
        df = df.withColumn(
            "operation_type",
            when(col("cdc_operation") == "c", lit("INSERT"))
            .when(col("cdc_operation") == "r", lit("READ"))
            .when(col("cdc_operation") == "u", lit("UPDATE"))
            .when(col("cdc_operation") == "d", lit("DELETE"))
            .otherwise(lit("UNKNOWN"))
        )

        return df

