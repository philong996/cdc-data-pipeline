"""Reader for silver layer Delta tables."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable
from cdc_pipelines.common.logger import get_logger

logger = get_logger(__name__)


class SilverReader:
    """
    Reader for silver layer Delta tables.
    Reads batch data from silver dimension and fact tables.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize silver reader.

        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        logger.info("Initialized SilverReader")

    def read_table(self, table_path: str, table_name: str) -> DataFrame:
        """
        Read a silver Delta table.

        Args:
            table_path: Path to the silver Delta table
            table_name: Name of the table (for logging)

        Returns:
            DataFrame containing the table data
        """
        logger.info(f"Reading silver table: {table_name} from {table_path}")
        
        if not DeltaTable.isDeltaTable(self.spark, table_path):
            logger.warning(f"Table does not exist: {table_path}")
            return None
        
        df = self.spark.read.format("delta").load(table_path)
        count = df.count()
        logger.info(f"Read {count} records from {table_name}")
        
        return df

    def read_current_dimension(self, table_path: str, table_name: str) -> DataFrame:
        """
        Read current records from an SCD Type 2 dimension table.

        Args:
            table_path: Path to the silver Delta table
            table_name: Name of the table (for logging)

        Returns:
            DataFrame containing only current (is_current = True) records
        """
        logger.info(f"Reading current dimension records from: {table_name}")
        
        df = self.read_table(table_path, table_name)
        
        if df is None:
            return None
        
        # Filter for current records
        current_df = df.filter(col("is_current") == True)
        count = current_df.count()
        logger.info(f"Found {count} current records in {table_name}")
        
        return current_df

    def read_fact_table(self, table_path: str, table_name: str, exclude_deleted: bool = True) -> DataFrame:
        """
        Read a fact table from silver layer.

        Args:
            table_path: Path to the silver Delta table
            table_name: Name of the table (for logging)
            exclude_deleted: Whether to exclude soft-deleted records (default: True)

        Returns:
            DataFrame containing the fact table data
        """
        logger.info(f"Reading fact table: {table_name}")
        
        df = self.read_table(table_path, table_name)
        
        if df is None:
            return None
        
        if exclude_deleted:
            df = df.filter(col("is_deleted") == False)
            count = df.count()
            logger.info(f"Filtered to {count} non-deleted records in {table_name}")
        
        return df
