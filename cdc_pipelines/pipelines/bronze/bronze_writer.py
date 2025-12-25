"""Bronze layer Delta Lake writer."""

from pyspark.sql import DataFrame
from typing import Dict, Any
from cdc_pipelines.common.logger import get_logger
from cdc_pipelines.common.constants import DELTA_MERGE_SCHEMA

logger = get_logger(__name__)


class BronzeWriter:
    """Writes streaming data to bronze Delta tables."""

    def __init__(self, config: Dict[str, Any], table_config: Dict[str, Any]):
        """
        Initialize bronze writer.

        Args:
            config: Main configuration dictionary
            table_config: Table-specific configuration
        """
        self.config = config
        self.table_config = table_config
        self.delta_config = config.get("delta", {})
        self.checkpoint_config = config.get("checkpoints", {})

    def write_stream(
        self,
        df: DataFrame,
        table_name: str,
        trigger_interval: str = "10 seconds"
    ):
        """
        Write streaming DataFrame to Delta table.

        Args:
            df: Streaming DataFrame to write
            table_name: Name of the target table
            trigger_interval: Trigger interval for micro-batches
            
        Returns:
            StreamingQuery object
        """
        bronze_path = self.delta_config.get("bronze_path")
        checkpoint_path = self.checkpoint_config.get("bronze_path")

        if not bronze_path:
            raise ValueError("Bronze path not configured")

        table_path = f"{bronze_path}/{table_name}"
        checkpoint_location = f"{checkpoint_path}/{table_name}"

        logger.info(f"Writing stream to bronze table: {table_name}")
        logger.info(f"Table path: {table_path}")
        logger.info(f"Checkpoint location: {checkpoint_location}")

        # Get partition columns if specified
        partition_columns = self.table_config.get("partition_columns", [])

        # Write to Delta
        query = (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location)
            .option("mergeSchema", DELTA_MERGE_SCHEMA)
            .trigger(processingTime=trigger_interval)
        )

        # Add partitioning if specified
        if partition_columns:
            query = query.partitionBy(*partition_columns)

        # Start the stream
        stream_query = query.start(table_path)

        logger.info(f"Stream started for table: {table_name}")
        logger.info(f"Query ID: {stream_query.id}")

        # Return the query object so caller can manage it
        return stream_query

    def write_batch(self, df: DataFrame, table_name: str, mode: str = "append") -> None:
        """
        Write batch DataFrame to Delta table.

        Args:
            df: Batch DataFrame to write
            table_name: Name of the target table
            mode: Write mode (append, overwrite, etc.)
        """
        bronze_path = self.delta_config.get("bronze_path")
        table_path = f"{bronze_path}/{table_name}"

        logger.info(f"Writing batch to bronze table: {table_name}")
        logger.info(f"Table path: {table_path}")
        logger.info(f"Write mode: {mode}")

        # Get partition columns if specified
        partition_columns = self.table_config.get("partition_columns", [])

        writer = df.write.format("delta").mode(mode).option("mergeSchema", DELTA_MERGE_SCHEMA)

        # Add partitioning if specified
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        writer.save(table_path)

        logger.info(f"Batch write completed for table: {table_name}")
