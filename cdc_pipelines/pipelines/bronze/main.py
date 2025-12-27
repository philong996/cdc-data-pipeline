"""Bronze layer pipeline entry point."""

import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from cdc_pipelines.common.logger import setup_logging
from cdc_pipelines.pipelines.bronze.bronze_pipeline import BronzePipeline


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
        environment: Environment for configuration (default: dev)
    """
    # Initialize pipeline
    pipeline = BronzePipeline(config_path, environment)

    try:
        # Setup logging
        logging_config = pipeline.config.get("logging", {})
        setup_logging(
            log_level=logging_config.get("level", "INFO"),
        )

        # Run pipeline
        pipeline.run(table_name)

    finally:
        # Stop Spark session
        try:
            pipeline.logger.info("Stopping Spark session...")
            pipeline.spark.stop()
            pipeline.logger.info("Spark session stopped. Pipeline shutdown complete.")
        except Exception as e:
            pipeline.logger.error(f"Error stopping Spark session: {str(e)}")


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
