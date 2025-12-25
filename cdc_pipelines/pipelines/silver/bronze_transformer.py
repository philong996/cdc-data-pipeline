"""Bronze layer transformations."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, when, coalesce, get_json_object
from typing import Dict, Any
from cdc_pipelines.common.logger import get_logger
from cdc_pipelines.common.constants import (
    COL_KAFKA_TOPIC,
    COL_KAFKA_PARTITION,
    COL_KAFKA_OFFSET,
    COL_KAFKA_TIMESTAMP,
    COL_INGESTION_TIMESTAMP,
    COL_CDC_OPERATION,
)

logger = get_logger(__name__)


class BronzeTransformer:
    """Transforms raw Kafka data for bronze layer."""

    def __init__(self, table_config: Dict[str, Any]):
        """
        Initialize bronze transformer.

        Args:
            table_config: Table configuration dictionary
        """
        self.table_config = table_config
        self.table_name = None

    def transform(self, df: DataFrame, table_name: str) -> DataFrame:
        """
        Apply bronze layer transformations.

        Args:
            df: Raw DataFrame with parsed CDC envelope
            table_name: Name of the target table

        Returns:
            Transformed DataFrame ready for bronze layer
        """
        self.table_name = table_name
        logger.info(f"Applying bronze transformations for table: {table_name}")

        # Add Kafka metadata
        df = self._add_kafka_metadata(df)

        # Flatten the payload
        df = self._flatten_payload(df)

        # Add ingestion metadata
        df = self._add_ingestion_metadata(df)

        logger.info(f"Bronze transformations completed for: {table_name}")

        return df

    def _add_kafka_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add Kafka metadata columns.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with Kafka metadata
        """
        df = df.withColumn(COL_KAFKA_TOPIC, col("topic"))
        df = df.withColumn(COL_KAFKA_PARTITION, col("partition"))
        df = df.withColumn(COL_KAFKA_OFFSET, col("offset"))
        df = df.withColumn(COL_KAFKA_TIMESTAMP, col("timestamp"))

        return df

    def _flatten_payload(self, df: DataFrame) -> DataFrame:
        """
        Flatten the CDC payload based on operation type.

        Args:
            df: Input DataFrame with 'after' and 'before' columns

        Returns:
            DataFrame with flattened payload
        """
        # For INSERT and UPDATE operations, use 'after'
        # For DELETE operations, use 'before'
        # Parse JSON payload
        df = df.withColumn(
            "payload_data",
            when(col("cdc_operation").isin(["c", "r", "u"]), col("after"))
            .otherwise(col("before"))
        )

        # Extract individual fields from JSON payload
        # This will be customized based on table schema
        schema = self.table_config.get("schema", {})

        for field_name, field_type in schema.items():

            df = df.withColumn(
                field_name,
                get_json_object(col("payload_data"), f"$.{field_name}")
            )

        return df

    def _add_ingestion_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add ingestion metadata columns.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with ingestion metadata
        """

        df = df.withColumn(COL_INGESTION_TIMESTAMP, current_timestamp())

        # Map CDC operation to readable format
        df = df.withColumn(
            COL_CDC_OPERATION,
            when(col("cdc_operation") == "c", "INSERT")
            .when(col("cdc_operation") == "r", "READ")
            .when(col("cdc_operation") == "u", "UPDATE")
            .when(col("cdc_operation") == "d", "DELETE")
            .otherwise("UNKNOWN")
        )

        # Add CDC timestamp
        df = df.withColumn(
            "cdc_timestamp",
            (col("source_ts_ms") / 1000).cast("timestamp")
        )

        return df

    def select_columns(self, df: DataFrame) -> DataFrame:
        """
        Select final columns for bronze table.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with selected columns
        """
        # Get data columns from schema
        data_columns = list(self.table_config.get("schema", {}).keys())

        # Metadata columns
        metadata_columns = [
            COL_KAFKA_TOPIC,
            COL_KAFKA_PARTITION,
            COL_KAFKA_OFFSET,
            COL_KAFKA_TIMESTAMP,
            COL_INGESTION_TIMESTAMP,
            COL_CDC_OPERATION,
            "cdc_timestamp",
        ]

        all_columns = data_columns + metadata_columns

        return df.select(*all_columns)
