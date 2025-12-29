"""Main entry point for gold layer pipeline."""

import argparse
import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from cdc_pipelines.common.logger import get_logger
from cdc_pipelines.pipelines.gold.gold_pipeline import GoldPipeline

logger = get_logger(__name__)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Gold layer batch pipeline")
    parser.add_argument(
        "--config",
        type=str,
        default="/home/longnguyen/cdc-data-pipeline/config/pipeline_config",
        help="Path to configuration file",
    )
    parser.add_argument(
        "--env",
        type=str,
        default="dev",
        help="Environment for configuration (default: dev)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=900,  # 15 minutes in seconds
        help="Execution interval in seconds (default: 900 = 15 minutes)",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run once and exit (default: run continuously)",
    )
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    try:
        pipeline = GoldPipeline(args.config, environment=args.env)
        
        if args.run_once:
            # Run once and exit
            logger.info("Running gold pipeline once...")
            pipeline.run()
            logger.info("Gold pipeline completed")
        else:
            # Run continuously with specified interval
            logger.info(f"Starting gold pipeline with {args.interval}s interval...")
            
            while True:
                try:
                    pipeline.run()
                    logger.info(f"Sleeping for {args.interval} seconds...")
                    time.sleep(args.interval)
                except KeyboardInterrupt:
                    logger.info("Received interrupt signal, stopping...")
                    break
                except Exception as e:
                    logger.error(f"Error in pipeline execution: {str(e)}", exc_info=True)
                    logger.info(f"Waiting {args.interval} seconds before retry...")
                    time.sleep(args.interval)
        
        pipeline.stop()
        
    except Exception as e:
        logger.error(f"Gold pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
