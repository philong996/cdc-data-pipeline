"""Bronze layer reader for silver pipeline."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    row_number,
    desc,
    expr,
    get_json_object,
    to_json,
    date_add,
    lit,
)
from pyspark.sql.window import Window
from cdc_pipelines.common.logger import get_logger
from cdc_pipelines.common.schemas import SOURCE_SCHEMAS, DEBEZIUM_SCHEMA
from cdc_pipelines.common.constants import (
    COL_CDC_TIMESTAMP,
    COL_CDC_OPERATION,
)

logger = get_logger(__name__)


class BronzeReader:
    """Reads bronze Delta tables and prepares data for silver processing."""

    def __init__(self, spark: SparkSession):
        """
        Initialize bronze reader.

        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        logger.info("Initialized BronzeReader")

    def read_bronze_batch(
        self,
        bronze_path: str,
        table_name: str,
    ) -> DataFrame:
        """
        Read bronze Delta table as batch DataFrame for debugging.

        Args:
            bronze_path: Path to bronze Delta table
            table_name: Name of the table

        Returns:
            Batch DataFrame
        """
        logger.info(f"Reading bronze batch from: {bronze_path}")

        # Read bronze Delta table as batch
        df = (
            self.spark.read
            .format("delta")
            .load(bronze_path)
        )

        logger.info(f"Bronze batch loaded for table: {table_name}")
        return df

    def read_bronze_stream(
        self,
        bronze_path: str,
        table_name: str,
        checkpoint_path: str = None,
    ) -> DataFrame:
        """
        Read bronze Delta table as streaming DataFrame.

        Args:
            bronze_path: Path to bronze Delta table
            table_name: Name of the table
            checkpoint_path: Checkpoint location for stream

        Returns:
            Streaming DataFrame
        """
        logger.info(f"Reading bronze stream from: {bronze_path}")

        # Read bronze Delta table as stream
        df = (
            self.spark.readStream
            .format("delta")
            .option("ignoreChanges", "true")  # For Delta Lake changes
            .option("ignoreDeletes", "true")  # For Delta Lake deletes
            .load(bronze_path)
        )

        logger.info(f"Bronze stream loaded for table: {table_name}")
        return df

    def parse_json_payload(
        self,
        df: DataFrame,
        table_name: str,
    ) -> DataFrame:
        """
        Parse JSON payload column into structured columns.
        Extracts data from Debezium envelope (payload.after or payload.before).

        Args:
            df: Bronze DataFrame with json_payload column
            table_name: Name of the table to get schema

        Returns:
            DataFrame with parsed columns
        """
        logger.info(f"Parsing JSON payload for table: {table_name}")

        # Get schema for table
        if table_name not in SOURCE_SCHEMAS:
            raise ValueError(f"Schema not found for table: {table_name}")

        schema = SOURCE_SCHEMAS[table_name]

        # filter null json_payload
        df = df.filter(col("json_payload").isNotNull())

        # Parse the full Debezium message with DEBEZIUM_SCHEMA
        df = df.withColumn(
            "debezium_payload",
            from_json(col("json_payload"), DEBEZIUM_SCHEMA)
        )
        
        # Extract CDC metadata from parsed structure (access through payload prefix)
        df = df.select(
            "*",
            col("debezium_payload.payload.op").alias(COL_CDC_OPERATION),
            (col("debezium_payload.payload.source.ts_ms") / 1000).cast("timestamp").alias(COL_CDC_TIMESTAMP),
        )

        # Extract the appropriate payload based on CDC operation
        # For DELETE (d), use before (already a JSON string)
        # For INSERT/UPDATE/READ (c/u/r), use after (already a JSON string)
        # The before/after fields are already JSON strings from DEBEZIUM_SCHEMA
        df = df.withColumn(
            "payload_data",
            expr("""
                CASE 
                    WHEN cdc_operation = 'd' THEN debezium_payload.payload.before
                    ELSE debezium_payload.payload.after
                END
            """)
        )

        # Parse the extracted payload into structured columns
        # payload_data is already a JSON string, so we can parse it directly
        df = df.withColumn(
            "parsed_data",
            from_json(col("payload_data"), schema)
        )

        # Flatten parsed data into individual columns
        for field in schema.fields:
            df = df.withColumn(
                field.name,
                col(f"parsed_data.{field.name}")
            )

        # Convert Debezium date fields (integer days since epoch) to DateType
        # For orders table, order_date is sent as days since 1970-01-01
        if table_name == "orders" and "order_date" in [f.name for f in schema.fields]:
            df = df.withColumn(
                "order_date",
                date_add(lit('1970-01-01').cast('date'), col('order_date'))
            )

        # Drop intermediate columns
        df = df.drop("parsed_data", "payload_data", "debezium_payload")

        logger.info(f"JSON payload parsed for table: {table_name}")
        return df

    def deduplicate_by_key(
        self,
        df: DataFrame,
        primary_keys: list,
        order_by: str = COL_CDC_TIMESTAMP,
    ) -> DataFrame:
        """
        Deduplicate records by primary key, keeping the latest based on order_by column.

        Args:
            df: DataFrame to deduplicate
            primary_keys: List of primary key columns
            order_by: Column to order by (default: cdc_timestamp)

        Returns:
            Deduplicated DataFrame
        """
        logger.info(
            f"Deduplicating by primary keys: {primary_keys}, ordering by: {order_by}"
        )

        # Create window partitioned by primary keys, ordered by timestamp descending
        window_spec = Window.partitionBy(*primary_keys).orderBy(desc(order_by))

        # Add row number
        df = df.withColumn("row_num", row_number().over(window_spec))

        # Keep only the first row (most recent)
        df = df.filter(col("row_num") == 1).drop("row_num")

        logger.info("Deduplication completed")
        return df

    def apply_watermark(
        self,
        df: DataFrame,
        event_time_column: str = COL_CDC_TIMESTAMP,
        delay_threshold: str = "1 hour",
    ) -> DataFrame:
        """
        Apply watermarking for handling late data in streaming.

        Args:
            df: Streaming DataFrame
            event_time_column: Column to use for event time
            delay_threshold: How late data can arrive (e.g., "1 hour", "10 minutes")

        Returns:
            DataFrame with watermark applied
        """
        logger.info(
            f"Applying watermark on {event_time_column} with threshold: {delay_threshold}"
        )

        df = df.withWatermark(event_time_column, delay_threshold)

        logger.info("Watermark applied")
        return df

    def read_and_prepare(
        self,
        bronze_path: str,
        table_name: str,
        primary_keys: list,
        apply_dedup: bool = True,
        watermark_delay: str = "1 hour",
        batch_mode: bool = False,
    ) -> DataFrame:
        """
        Read bronze table and prepare for silver processing.
        Combines read, parse, deduplicate operations.

        Args:
            bronze_path: Path to bronze Delta table
            table_name: Name of the table
            primary_keys: List of primary key columns for deduplication
            apply_dedup: Whether to apply deduplication (use False for streaming with stateful operations)
            watermark_delay: Watermark delay threshold
            batch_mode: If True, read as batch for debugging; if False, read as stream

        Returns:
            Prepared DataFrame ready for silver processing
        """
        logger.info(f"Reading and preparing bronze data for table: {table_name} (batch_mode={batch_mode})")

        # Read bronze data
        if batch_mode:
            df = self.read_bronze_batch(bronze_path, table_name)
        else:
            df = self.read_bronze_stream(bronze_path, table_name)

        # Parse JSON payload
        df = self.parse_json_payload(df, table_name)

        # Apply watermark only for streaming mode
        if not batch_mode:
            df = self.apply_watermark(df, delay_threshold=watermark_delay)

        # Optionally deduplicate
        if apply_dedup:
            if not batch_mode:
                logger.warning(
                    "Deduplication in streaming mode may cause issues with stateful operations. "
                    "Consider deduplicating within foreachBatch instead."
                )
            else:
                df = self.deduplicate_by_key(df, primary_keys)

        logger.info(f"Bronze data prepared for table: {table_name}")
        return df
