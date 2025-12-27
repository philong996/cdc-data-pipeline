"""Bronze layer pipeline orchestration."""

from typing import Dict, List, Optional, Tuple

from pyspark.sql.streaming import StreamingQuery

from cdc_pipelines.common.config_loader import ConfigLoader
from cdc_pipelines.common.spark_session import get_spark_session
from cdc_pipelines.common.logger import PipelineLogger
from cdc_pipelines.pipelines.bronze.kafka_reader import KafkaReader
from cdc_pipelines.pipelines.bronze.bronze_writer import BronzeWriter

PIPELINE_LAYER = "bronze"


class BronzePipeline:
    """
    Orchestrates the bronze layer pipeline.
    Reads from Kafka and writes to bronze Delta tables.
    """

    def __init__(self, config_path: str, environment: str = "dev"):
        """
        Initialize bronze pipeline.

        Args:
            config_path: Path to configuration file
            environment: Environment for configuration (default: dev)
        """
        config_loader = ConfigLoader(config_path, environment=environment)
        self.config = config_loader.load_config()
        self.config_loader = config_loader

        self.logger = PipelineLogger(__name__, PIPELINE_LAYER, PIPELINE_LAYER)
        self.logger.info(f"Starting {PIPELINE_LAYER} layer pipeline")

        self.spark = get_spark_session(self.config, name=PIPELINE_LAYER)
        self.kafka_reader = KafkaReader(self.spark, self.config)
        self.queries: List[Tuple[str, StreamingQuery]] = []  # Track active streaming queries

        self.logger.info("Initialized BronzePipeline")

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

    def process_table(self, table_name: str, table_config: Dict):
        """
        Process a single table from Kafka to Bronze Delta.

        Args:
            table_name: Name of the table
            table_config: Table configuration
        """
        self.logger.info(f"Starting pipeline for table: {table_name}")

        # Get topic for this table
        topic = table_config.get("source_topic")
        if not topic:
            self.logger.error(f"No source topic configured for table: {table_name}")
            raise ValueError(f"No source topic configured for table: {table_name}")

        # Get trigger interval
        bronze_config = self.config.get("bronze", {})
        trigger_interval = bronze_config.get("trigger_interval", "10 seconds")

        # Read from Kafka with unique consumer group per table
        self.logger.info(f"Reading from Kafka topic: {topic}")
        consumer_group = f"cdc-streaming-bronze-{table_name}"
        bronze_df = self.kafka_reader.read_stream([topic], group_id=consumer_group)

        # Write to Delta
        writer = BronzeWriter(self.config, table_config)
        self.logger.info(f"Starting stream write for table: {table_name}")
        query = writer.write_stream(bronze_df, table_name, trigger_interval)

        # Track the query
        if query:
            self.queries.append((table_name, query))
            self.logger.info(f"Started streaming query for table: {table_name}")

    def run(self, table_name: Optional[str] = None):
        """
        Run the bronze pipeline for all configured tables or a specific table.

        Args:
            table_name: Specific table to process (optional, processes all if not specified)
        """
        self.logger.info("Starting bronze layer pipeline...")

        # Determine which tables to process
        tables_to_process = {}
        if table_name:
            # Process single table
            table_config = self.get_table_config(table_name)
            tables_to_process[table_name] = table_config
        else:
            # Process all tables
            tables_to_process = self.config.get("tables", {})

        self.logger.info(f"Processing {len(tables_to_process)} table(s)")

        # Process each table
        for tbl_name, tbl_config in tables_to_process.items():
            try:
                self.process_table(tbl_name, tbl_config)
            except Exception as e:
                self.logger.error(
                    f"Error processing table {tbl_name}: {str(e)}",
                    exc_info=True,
                )
                raise

        self.logger.info(
            f"Bronze pipeline started successfully. {len(self.queries)} queries running."
        )

        # Wait for all queries to complete
        try:
            self.logger.info("All streams started. Waiting for termination...")
            self.logger.info("Press Ctrl+C to stop gracefully")

            for tbl_name, query in self.queries:
                query.awaitTermination()

        except KeyboardInterrupt:
            self.logger.info("Received KeyboardInterrupt. Stopping streams gracefully...")
            self.stop()
            self.logger.info("All streams stopped")

    def stop(self):
        """Stop all streaming queries."""
        self.logger.info("Stopping all streaming queries...")
        for tbl_name, query in self.queries:
            try:
                if query.isActive:
                    self.logger.info(f"Stopping stream for table: {tbl_name}")
                    query.stop()
                    self.logger.info(f"Stream stopped for table: {tbl_name}")
            except Exception as e:
                self.logger.error(f"Error stopping stream for {tbl_name}: {str(e)}")

        self.logger.info("All queries stopped")
