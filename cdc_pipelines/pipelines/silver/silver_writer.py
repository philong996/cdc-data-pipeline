"""Silver layer writers for dimension and fact tables."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    when,
    coalesce,
    row_number,
    max as spark_max,
    md5,
    concat_ws,
    expr,
    monotonically_increasing_id,
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from cdc_pipelines.common.logger import get_logger
from cdc_pipelines.common.constants import (
    COL_CDC_OPERATION,
    COL_CDC_TIMESTAMP,
    COL_INGESTION_TIMESTAMP,
    META_COLS,
)

logger = get_logger(__name__)


class SCD2Writer:
    """
    Writer for dimension tables using SCD Type 2 (Slowly Changing Dimension).
    Tracks historical changes with effective dates and versions.
    """

    def __init__(
        self,
        spark: SparkSession,
        table_name: str,
        silver_path: str,
        business_keys: list,
        attribute_columns: list,
        surrogate_key_name: str = None,
    ):
        """
        Initialize SCD2 writer.

        Args:
            spark: SparkSession instance
            table_name: Name of the silver table
            silver_path: Path to silver Delta table
            business_keys: List of natural key columns (e.g., ['product_id'])
            attribute_columns: List of attribute columns to track changes
            surrogate_key_name: Name of surrogate key column (e.g., 'product_sk')
        """
        self.spark = spark
        self.table_name = table_name
        self.silver_path = silver_path
        self.business_keys = business_keys
        self.attribute_columns = attribute_columns
        self.surrogate_key_name = (
            surrogate_key_name or f"{table_name.rstrip('s')}_sk"
        )
        logger.info(f"Initialized SCD2Writer for table: {table_name}")

    def _calculate_row_hash(self, df: DataFrame) -> DataFrame:
        """Calculate MD5 hash of attribute columns for change detection."""
        df = df.withColumn(
            "row_hash",
            md5(concat_ws("|", *[col(c).cast("string") for c in self.attribute_columns]))
        )
        return df

    def _process_batch(self, batch_df: DataFrame, batch_id: int):
        """
        Process micro-batch with SCD2 merge logic.

        Args:
            batch_df: Micro-batch DataFrame
            batch_id: Batch identifier
        """
        logger.info(f"Processing batch {batch_id} for table: {self.table_name}")

        # Deduplicate within batch by business key, keeping latest by cdc_timestamp
        window_spec = Window.partitionBy(*self.business_keys).orderBy(
            col(COL_CDC_TIMESTAMP).desc()
        )
        batch_df = batch_df.withColumn("row_num", row_number().over(window_spec))
        batch_df = batch_df.filter(col("row_num") == 1).drop("row_num")

        # Calculate row hash for change detection
        batch_df = self._calculate_row_hash(batch_df)

        # Check if table exists
        if DeltaTable.isDeltaTable(self.spark, self.silver_path):
            self._merge_scd2(batch_df)
        else:
            self._initial_load(batch_df)

        logger.info(f"Batch {batch_id} processed successfully for {self.table_name}")

    def _initial_load(self, df: DataFrame):
        """Handle initial load - create first version of all records."""
        logger.info(f"Performing initial load for table: {self.table_name}")

        # Generate surrogate keys
        df = df.withColumn(
            self.surrogate_key_name,
            monotonically_increasing_id()
        )

        # Add SCD2 metadata
        df = df.withColumn("effective_from", col(COL_CDC_TIMESTAMP))
        df = df.withColumn("effective_to", lit(None).cast("timestamp"))
        df = df.withColumn("is_current", lit(True))
        df = df.withColumn("version", lit(1))

        # Add lineage tracking
        df = df.withColumn("created_from_bronze_ts", col(COL_INGESTION_TIMESTAMP))
        df = df.withColumn(
            "source_operation",
            when(col(COL_CDC_OPERATION) == "c", lit("INSERT"))
            .when(col(COL_CDC_OPERATION) == "r", lit("READ"))
            .when(col(COL_CDC_OPERATION) == "u", lit("UPDATE"))
            .when(col(COL_CDC_OPERATION) == "d", lit("DELETE"))
            .otherwise(lit("UNKNOWN"))
        )

        # Select final columns
        final_columns = (
            self.business_keys
            + [self.surrogate_key_name]
            + self.attribute_columns
            + META_COLS
            + [
                "effective_from",
                "effective_to",
                "is_current",
                "version",
                "created_from_bronze_ts",
                "source_operation",
            ]
        )

        df = df.select(*final_columns)

        # Write initial data
        df.write.format("delta").mode("overwrite").save(self.silver_path)

        logger.info(f"Initial load completed for table: {self.table_name}")

    def _merge_scd2(self, batch_df: DataFrame):
        """
        Merge batch data using SCD2 logic.
        - INSERT: New records get version 1
        - UPDATE: If attributes changed, close current version and insert new version
        - DELETE: Close current version, mark as deleted
        - READ: Treat same as INSERT for initial snapshot
        """
        logger.info(f"Performing SCD2 merge for table: {self.table_name}")

        delta_table = DeltaTable.forPath(self.spark, self.silver_path)

        # Build merge condition on business keys (only match current records)
        merge_condition = " AND ".join(
            [f"target.{key} = source.{key}" for key in self.business_keys]
        ) + " AND target.is_current = true"

        # Get max surrogate key for generating new ones
        max_sk = (
            self.spark.read.format("delta")
            .load(self.silver_path)
            .agg(spark_max(self.surrogate_key_name))
            .collect()[0][0]
        )
        max_sk = max_sk if max_sk is not None else 0

        # Add temporary surrogate keys to batch (will be used for new records)
        window_spec = Window.orderBy(lit(1))
        batch_df = batch_df.withColumn(
            "_temp_sk",
            lit(max_sk) + row_number().over(window_spec)
        )

        # Perform merge
        (
            delta_table.alias("target")
            .merge(
                batch_df.alias("source"),
                merge_condition,
            )
            .whenMatchedUpdate(
                condition=f"""
                    (target.row_hash != source.row_hash 
                     AND source.{COL_CDC_OPERATION} IN ('u', 'c', 'r'))
                    OR source.{COL_CDC_OPERATION} = 'd'
                """,
                set={
                    "is_current": "false",
                    "effective_to": f"source.{COL_CDC_TIMESTAMP}",
                    "source_operation": f"""
                        CASE 
                            WHEN source.{COL_CDC_OPERATION} = 'c' THEN 'INSERT'
                            WHEN source.{COL_CDC_OPERATION} = 'r' THEN 'READ'
                            WHEN source.{COL_CDC_OPERATION} = 'u' THEN 'UPDATE'
                            WHEN source.{COL_CDC_OPERATION} = 'd' THEN 'DELETE'
                            ELSE 'UNKNOWN'
                        END
                    """,
                },
            )
            .whenNotMatchedInsert(
                condition=f"source.{COL_CDC_OPERATION} IN ('c', 'r', 'u')",
                values={
                    **{key: f"source.{key}" for key in self.business_keys},
                    self.surrogate_key_name: "source._temp_sk",
                    **{attr: f"source.{attr}" for attr in self.attribute_columns},
                    **{meta_col: f"source.{meta_col}" for meta_col in META_COLS},
                    "effective_from": f"source.{COL_CDC_TIMESTAMP}",
                    "effective_to": "NULL",
                    "is_current": "true",
                    "version": "1",
                    "created_from_bronze_ts": f"source.{COL_INGESTION_TIMESTAMP}",
                    "source_operation": f"""
                        CASE 
                            WHEN source.{COL_CDC_OPERATION} = 'c' THEN 'INSERT'
                            WHEN source.{COL_CDC_OPERATION} = 'r' THEN 'READ'
                            WHEN source.{COL_CDC_OPERATION} = 'u' THEN 'UPDATE'
                            ELSE 'UNKNOWN'
                        END
                    """
                },
            )
            .execute()
        )

        # Insert new versions for updated records
        # Find records that were just closed (effective_to was set)
        closed_records = (
            self.spark.read.format("delta")
            .load(self.silver_path)
            .filter(
                (col("is_current") == False)
                & (col("effective_to") == col(COL_CDC_TIMESTAMP))
            )
        )

        # If there are closed records, we need to insert new versions
        if closed_records.count() > 0:
            # Join with source to get new attribute values
            new_versions = closed_records.join(
                batch_df,
                self.business_keys,
                "inner"
            ).filter(
                col(f"source.{COL_CDC_OPERATION}") != "d"  # Don't create new version for deletes
            )

            # Generate new surrogate keys
            max_sk_after_merge = (
                self.spark.read.format("delta")
                .load(self.silver_path)
                .agg(spark_max(self.surrogate_key_name))
                .collect()[0][0]
            )

            window_spec = Window.orderBy(lit(1))
            new_versions = new_versions.withColumn(
                self.surrogate_key_name,
                lit(max_sk_after_merge) + row_number().over(window_spec)
            )

            # Build new version records
            new_versions = new_versions.select(
                *[col(f"source.{key}").alias(key) for key in self.business_keys],
                col(self.surrogate_key_name),
                *[col(f"source.{attr}").alias(attr) for attr in self.attribute_columns],
                *[col(f"source.{meta_col}").alias(meta_col) for meta_col in META_COLS],
                col(f"source.{COL_CDC_TIMESTAMP}").alias("effective_from"),
                lit(None).cast("timestamp").alias("effective_to"),
                lit(True).alias("is_current"),
                (col("target.version") + 1).alias("version"),
                col("target.created_from_bronze_ts"),
                when(col(f"source.{COL_CDC_OPERATION}") == "u", lit("UPDATE"))
                .when(col(f"source.{COL_CDC_OPERATION}") == "c", lit("INSERT"))
                .otherwise(lit("READ"))
                .alias("source_operation")
            )

            # Append new versions
            new_versions.write.format("delta").mode("append").save(self.silver_path)

        logger.info(f"SCD2 merge completed for table: {self.table_name}")

    def write_stream(
        self,
        stream_df: DataFrame,
        checkpoint_path: str,
        trigger_interval: str = "10 seconds",
    ):
        """
        Write streaming DataFrame to silver layer with SCD2 logic.

        Args:
            stream_df: Streaming DataFrame from bronze
            checkpoint_path: Checkpoint location
            trigger_interval: Trigger interval for micro-batches
        """
        logger.info(f"Starting SCD2 stream write for table: {self.table_name}")

        query = (
            stream_df.writeStream.foreachBatch(self._process_batch)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=trigger_interval)
            .start()
        )

        logger.info(f"Stream started for table: {self.table_name}")
        return query


class AppendOnlyWriter:
    """
    Writer for fact tables using append-only pattern with soft deletes.
    Tracks updates and maintains change history.
    """

    def __init__(
        self,
        spark: SparkSession,
        table_name: str,
        silver_path: str,
        primary_keys: list,
        attribute_columns: list,
    ):
        """
        Initialize append-only writer.

        Args:
            spark: SparkSession instance
            table_name: Name of the silver table
            silver_path: Path to silver Delta table
            primary_keys: List of primary key columns
            attribute_columns: List of attribute columns
        """
        self.spark = spark
        self.table_name = table_name
        self.silver_path = silver_path
        self.primary_keys = primary_keys
        self.attribute_columns = attribute_columns
        logger.info(f"Initialized AppendOnlyWriter for table: {table_name}")

    def _process_batch(self, batch_df: DataFrame, batch_id: int):
        """
        Process micro-batch with append-only merge logic.

        Args:
            batch_df: Micro-batch DataFrame
            batch_id: Batch identifier
        """
        logger.info(f"Processing batch {batch_id} for table: {self.table_name}")

        # Deduplicate within batch by primary key, keeping latest by cdc_timestamp
        window_spec = Window.partitionBy(*self.primary_keys).orderBy(
            col(COL_CDC_TIMESTAMP).desc()
        )
        batch_df = batch_df.withColumn("row_num", row_number().over(window_spec))
        batch_df = batch_df.filter(col("row_num") == 1).drop("row_num")

        # Check if table exists
        if DeltaTable.isDeltaTable(self.spark, self.silver_path):
            self._merge_append_only(batch_df)
        else:
            self._initial_load(batch_df)

        logger.info(f"Batch {batch_id} processed successfully for {self.table_name}")

    def _initial_load(self, df: DataFrame):
        """Handle initial load - create first version of all records."""
        logger.info(f"Performing initial load for table: {self.table_name}")

        # Add change tracking fields
        df = df.withColumn("created_at", col(COL_CDC_TIMESTAMP))
        df = df.withColumn("updated_at", col(COL_CDC_TIMESTAMP))
        df = df.withColumn(
            "is_deleted",
            when(col(COL_CDC_OPERATION) == "d", lit(True)).otherwise(lit(False))
        )
        df = df.withColumn(
            "deleted_at",
            when(col(COL_CDC_OPERATION) == "d", col(COL_CDC_TIMESTAMP))
            .otherwise(lit(None).cast("timestamp"))
        )

        # Add lineage tracking
        df = df.withColumn("first_seen_bronze_ts", col(COL_CDC_TIMESTAMP))
        df = df.withColumn("update_count", lit(0))

        # Select final columns
        final_columns = (
            self.primary_keys
            + self.attribute_columns
            + META_COLS
            + [
                "created_at",
                "updated_at",
                "is_deleted",
                "deleted_at",
                "first_seen_bronze_ts",
                "update_count",
            ]
        )

        df = df.select(*final_columns)

        # Write initial data
        df.write.format("delta").mode("overwrite").save(self.silver_path)

        logger.info(f"Initial load completed for table: {self.table_name}")

    def _merge_append_only(self, batch_df: DataFrame):
        """
        Merge batch data using append-only logic with soft deletes.
        - INSERT/READ: Add new record
        - UPDATE: Update existing record, increment update_count
        - DELETE: Mark record as deleted with soft delete flag
        """
        logger.info(f"Performing append-only merge for table: {self.table_name}")

        delta_table = DeltaTable.forPath(self.spark, self.silver_path)

        # Build merge condition on primary keys
        merge_condition = " AND ".join(
            [f"target.{key} = source.{key}" for key in self.primary_keys]
        )

        # Perform merge
        (
            delta_table.alias("target")
            .merge(
                batch_df.alias("source"),
                merge_condition,
            )
            .whenMatchedUpdate(
                set={
                    **{attr: f"source.{attr}" for attr in self.attribute_columns},
                    "updated_at": f"source.{COL_CDC_TIMESTAMP}",
                    "is_deleted": f"CASE WHEN source.{COL_CDC_OPERATION} = 'd' THEN true ELSE false END",
                    "deleted_at": f"""
                        CASE 
                            WHEN source.{COL_CDC_OPERATION} = 'd' THEN source.{COL_CDC_TIMESTAMP}
                            ELSE target.deleted_at
                        END
                    """,
                    "update_count": "target.update_count + 1",
                }
            )
            .whenNotMatchedInsert(
                condition=f"source.{COL_CDC_OPERATION} IN ('c', 'r', 'u')",
                values={
                    **{key: f"source.{key}" for key in self.primary_keys},
                    **{attr: f"source.{attr}" for attr in self.attribute_columns},
                    "created_at": f"source.{COL_CDC_TIMESTAMP}",
                    "updated_at": f"source.{COL_CDC_TIMESTAMP}",
                    "is_deleted": "false",
                    "deleted_at": "NULL",
                    "first_seen_bronze_ts": f"source.{COL_CDC_TIMESTAMP}",
                    "update_count": "0",
                },
            )
            .execute()
        )

        logger.info(f"Append-only merge completed for table: {self.table_name}")

    def write_stream(
        self,
        stream_df: DataFrame,
        checkpoint_path: str,
        trigger_interval: str = "10 seconds",
    ):
        """
        Write streaming DataFrame to silver layer with append-only logic.

        Args:
            stream_df: Streaming DataFrame from bronze
            checkpoint_path: Checkpoint location
            trigger_interval: Trigger interval for micro-batches
        """
        logger.info(f"Starting append-only stream write for table: {self.table_name}")

        query = (
            stream_df.writeStream.foreachBatch(self._process_batch)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=trigger_interval)
            .start()
        )

        logger.info(f"Stream started for table: {self.table_name}")
        return query
