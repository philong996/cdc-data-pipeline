"""Bronze layer pipeline entry point."""

import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from cdc_pipelines.common.config_loader import ConfigLoader
from cdc_pipelines.common.spark_session import get_spark_session
from cdc_pipelines.common.logger import setup_logging, PipelineLogger
from cdc_pipelines.pipelines.bronze.kafka_reader import KafkaReader
from cdc_pipelines.pipelines.bronze.bronze_writer import BronzeWriter


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Bronze layer streaming pipeline")
    parser.add_argument(
        "--config",
        type=str,
        default="/home/longnguyen/cdc-data-pipeline/config/pipeline_config",
        help="Path to configuration file",
    )
    parser.add_argument(
        "--table",
        type=str,
        help="Specific table to process (optional, processes all if not specified)",
    )
    parser.add_argument(
        "--env",
        type=str,
        default="dev",
        help="Environment for configuration (default: dev)",
    )
    return parser.parse_args()


def run_bronze_pipeline(config_path: str, table_name: str = None, environment: str = "dev"):
    """
    Run the bronze layer pipeline.

    Args:
        config_path: Path to configuration file
        table_name: Specific table to process (optional)
    """
    # Load configuration
    config_loader = ConfigLoader(config_path, environment)
    config = config_loader.load_config()

    # Setup logging
    logging_config = config.get("logging", {})
    setup_logging(
        log_level=logging_config.get("level", "INFO"),
    )

    logger = PipelineLogger(__name__, "bronze", "bronze")
    logger.info("Starting bronze layer pipeline")

    # Create Spark session
    spark = get_spark_session(config)

    # Initialize components
    kafka_reader = KafkaReader(spark, config)

    # Get bronze configuration
    bronze_config = config.get("bronze", {})
    trigger_interval = bronze_config.get("trigger_interval", "10 seconds")

    # Determine which tables to process
    tables_to_process = {}
    if table_name:
        # Process single table
        table_config = config_loader.get_table_config(table_name)
        tables_to_process[table_name] = table_config
    else:
        # Process all tables
        tables_to_process = config.get("tables", {})

    logger.info(f"Processing {len(tables_to_process)} table(s)")

    # Track all streaming queries for graceful shutdown
    streaming_queries = []

    try:
        # Process each table
        for tbl_name, tbl_config in tables_to_process.items():
            try:
                logger.info(f"Starting pipeline for table: {tbl_name}")

                # Get topic for this table
                topic = tbl_config.get("source_topic")
                if not topic:
                    logger.error(f"No source topic configured for table: {tbl_name}")
                    continue

                # Read from Kafka with unique consumer group per table
                logger.info(f"Reading from Kafka topic: {topic}")
                consumer_group = f"cdc-streaming-bronze-{tbl_name}"
                bronze_df = kafka_reader.read_stream([topic], group_id=consumer_group)

                # Write to Delta
                writer = BronzeWriter(config, tbl_config)
                logger.info(f"Starting stream write for table: {tbl_name}")
                query = writer.write_stream(bronze_df, tbl_name, trigger_interval)
                
                # Track the query
                if query:
                    streaming_queries.append((tbl_name, query))

            except Exception as e:
                logger.exception(f"Error processing table {tbl_name}: {str(e)}")
                raise

        # Wait for all streams to finish (or until interrupted)
        logger.info(f"All streams started. Waiting for termination...")
        logger.info("Press Ctrl+C to stop gracefully")
        
        for tbl_name, query in streaming_queries:
            query.awaitTermination()
            
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt. Stopping streams gracefully...")
        
        # Stop all streaming queries
        for tbl_name, query in streaming_queries:
            try:
                if query.isActive:
                    logger.info(f"Stopping stream for table: {tbl_name}")
                    query.stop()
                    logger.info(f"Stream stopped for table: {tbl_name}")
            except Exception as e:
                logger.error(f"Error stopping stream for {tbl_name}: {str(e)}")
        
        logger.info("All streams stopped")
        
    finally:
        # Stop Spark session
        try:
            logger.info("Stopping Spark session...")
            spark.stop()
            logger.info("Spark session stopped. Pipeline shutdown complete.")
        except Exception as e:
            logger.error(f"Error stopping Spark session: {str(e)}")


def main():
    """Main entry point."""
    args = parse_args()

    try:
        run_bronze_pipeline(args.config, args.table, args.env)
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
