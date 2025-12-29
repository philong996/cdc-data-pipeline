"""Writers for gold layer aggregated tables."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable
from cdc_pipelines.common.logger import get_logger

logger = get_logger(__name__)


class GoldWriter:
    """
    Writer for gold layer aggregated tables.
    Uses complete overwrite strategy for batch aggregations.
    """

    def __init__(
        self,
        spark: SparkSession,
        table_name: str,
        gold_path: str,
    ):
        """
        Initialize gold writer.

        Args:
            spark: SparkSession instance
            table_name: Name of the gold table
            gold_path: Path to gold Delta table
        """
        self.spark = spark
        self.table_name = table_name
        self.gold_path = gold_path
        logger.info(f"Initialized GoldWriter for table: {table_name}")

    def write_complete(self, df: DataFrame):
        """
        Write DataFrame to gold table using complete overwrite strategy.
        Adds last_updated_at timestamp before writing.

        Args:
            df: DataFrame to write
        """
        logger.info(f"Writing {df.count()} records to gold table: {self.table_name}")
        
        # Add metadata timestamp
        df = df.withColumn("last_updated_at", current_timestamp())
        
        # Write with overwrite mode
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(self.gold_path)
        
        logger.info(f"Successfully wrote to {self.table_name} at {self.gold_path}")

    def write_merge(self, df: DataFrame, merge_keys: list):
        """
        Write DataFrame to gold table using merge (upsert) strategy.
        Useful for incremental updates to large tables.

        Args:
            df: DataFrame to write
            merge_keys: List of columns to use as merge keys
        """
        logger.info(f"Merging {df.count()} records into gold table: {self.table_name}")
        
        # Add metadata timestamp
        df = df.withColumn("last_updated_at", current_timestamp())
        
        if not DeltaTable.isDeltaTable(self.spark, self.gold_path):
            # First write - just write the data
            logger.info(f"First write - creating table {self.table_name}")
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(self.gold_path)
        else:
            # Table exists - perform merge
            delta_table = DeltaTable.forPath(self.spark, self.gold_path)
            
            # Build merge condition
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
            
            # Execute merge
            delta_table.alias("target") \
                .merge(
                    df.alias("source"),
                    merge_condition
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        
        logger.info(f"Successfully merged into {self.table_name}")

    def optimize_table(self):
        """
        Optimize the Delta table by compacting small files.
        Should be run periodically to maintain query performance.
        """
        if DeltaTable.isDeltaTable(self.spark, self.gold_path):
            logger.info(f"Optimizing table: {self.table_name}")
            delta_table = DeltaTable.forPath(self.spark, self.gold_path)
            delta_table.optimize().executeCompaction()
            logger.info(f"Optimization complete for {self.table_name}")
        else:
            logger.warning(f"Cannot optimize - table does not exist: {self.table_name}")
