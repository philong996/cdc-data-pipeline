from typing import Dict

from cdc_pipelines.common.config_loader import ConfigLoader
from cdc_pipelines.common.spark_session import get_spark_session
from cdc_pipelines.common.logger import PipelineLogger
from cdc_pipelines.pipelines.silver.bronze_reader import BronzeReader
from cdc_pipelines.pipelines.silver.silver_writer import SCD2Writer, AppendOnlyWriter

PIPELINE_LAYER = "silver"

class SilverPipeline:
    """
    Orchestrates the silver layer pipeline.
    Reads from bronze Delta tables and applies SCD2 or append-only patterns.
    """

    def __init__(self, config_path: str, environment: str = "dev"):
        """
        Initialize silver pipeline.

        Args:
            config_path: Path to configuration file
        """
        config_loader = ConfigLoader(config_path, environment=environment)
        self.config = config_loader.load_config()


        self.logger = PipelineLogger(__name__, PIPELINE_LAYER, PIPELINE_LAYER)
        self.logger.info(f"Starting {PIPELINE_LAYER} layer pipeline")

        self.spark = get_spark_session(
            self.config,
            name="silver"
        )
        self.bronze_reader = BronzeReader(self.spark)
        self.queries = []  # Track active streaming queries
    
        self.logger.info("Initialized SilverPipeline")

    def get_table_config(self, table_name: str) -> Dict:
        """
        Get configuration for a specific table.

        Args:
            table_name: Name of the table

        Returns:
            Table configuration dictionary
        """
        if table_name not in self.config["tables"]:
            raise ValueError(f"Table configuration not found: {table_name}")

        return self.config["tables"][table_name]

    def process_dimension_table(self, table_name: str, table_config: Dict):
        """
        Process a dimension table with SCD Type 2 logic.

        Args:
            table_name: Name of the table
            table_config: Table configuration
        """
        self.logger.info(f"Processing dimension table: {table_name}")

        # Get paths
        bronze_path = f"{self.config['delta']['bronze_path']}/{table_name}"
        silver_path = f"{self.config['delta']['silver_path']}/{table_name}"
        checkpoint_path = (
            f"{self.config['checkpoints']['silver_path']}/{table_name}"
        )

        # Get table metadata
        business_keys = table_config.get("business_keys", [])
        attribute_columns = table_config.get("attribute_columns", [])
        surrogate_key = table_config.get("surrogate_key")

        # Read from bronze
        bronze_df = self.bronze_reader.read_and_prepare(
            bronze_path=bronze_path,
            table_name=table_name,
            primary_keys=business_keys,
            apply_dedup=False,  # Dedup handled in foreachBatch
        )

        # Create SCD2 writer
        writer = SCD2Writer(
            spark=self.spark,
            table_name=table_name,
            silver_path=silver_path,
            business_keys=business_keys,
            attribute_columns=attribute_columns,
            surrogate_key_name=surrogate_key,
        )

        # Start streaming write
        trigger_interval = self.config.get("silver", {}).get(
            "trigger_interval", "10 seconds"
        )
        query = writer.write_stream(
            stream_df=bronze_df,
            checkpoint_path=checkpoint_path,
            trigger_interval=trigger_interval,
        )

        self.queries.append(query)
        self.logger.info(f"Started streaming query for dimension table: {table_name}")

    def process_fact_table(self, table_name: str, table_config: Dict):
        """
        Process a fact table with append-only logic.

        Args:
            table_name: Name of the table
            table_config: Table configuration
        """
        self.logger.info(f"Processing fact table: {table_name}")

        # Get paths
        bronze_path = f"{self.config['delta']['bronze_path']}/{table_name}"
        silver_path = f"{self.config['delta']['silver_path']}/{table_name}"
        checkpoint_path = (
            f"{self.config['checkpoints']['silver_path']}/{table_name}"
        )

        # Get table metadata
        primary_keys = table_config.get("primary_keys", [])
        attribute_columns = table_config.get("attribute_columns", [])

        # Read from bronze
        bronze_df = self.bronze_reader.read_and_prepare(
            bronze_path=bronze_path,
            table_name=table_name,
            primary_keys=primary_keys,
            apply_dedup=False,  # Dedup handled in foreachBatch
        )

        # Create append-only writer
        writer = AppendOnlyWriter(
            spark=self.spark,
            table_name=table_name,
            silver_path=silver_path,
            primary_keys=primary_keys,
            attribute_columns=attribute_columns,
        )

        # Start streaming write
        trigger_interval = self.config.get("silver", {}).get(
            "trigger_interval", "10 seconds"
        )
        query = writer.write_stream(
            stream_df=bronze_df,
            checkpoint_path=checkpoint_path,
            trigger_interval=trigger_interval,
        )

        self.queries.append(query)
        self.logger.info(f"Started streaming query for fact table: {table_name}")

    def run(self):
        """Run the silver pipeline for all configured tables."""
        self.logger.info("Starting silver pipeline...")
        # Process each table based on its type
        for table_name, table_config in self.config["tables"].items():
            table_type = table_config.get("table_type", "dimension")

            try:
                if table_type == "dimension":
                    self.process_dimension_table(table_name, table_config)
                elif table_type == "fact":
                    self.process_fact_table(table_name, table_config)
                else:
                    self.logger.warning(
                        f"Unknown table type '{table_type}' for table: {table_name}"
                    )

            except Exception as e:
                self.logger.error(
                    f"Error processing table {table_name}: {str(e)}",
                    exc_info=True,
                )
                raise

        self.logger.info(
            f"Silver pipeline started successfully. {len(self.queries)} queries running."
        )

        # Wait for all queries to complete
        try:
            for query in self.queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            self.logger.info("Stopping silver pipeline...")
            for query in self.queries:
                query.stop()
            self.logger.info("Silver pipeline stopped")

    def stop(self):
        """Stop all streaming queries."""
        self.logger.info("Stopping all streaming queries...")
        for query in self.queries:
            if query.isActive:
                query.stop()
        self.logger.info("All queries stopped")