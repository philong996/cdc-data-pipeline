"""Main entry point for silver layer pipeline."""

import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from cdc_pipelines.common.logger import get_logger
from cdc_pipelines.pipelines.silver.silver_pipeline import SilverPipeline

logger = get_logger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Silver layer streaming pipeline")
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
    return parser.parse_args()



def main():
    """Main entry point."""
    args = parse_args()

    try:
        pipeline = SilverPipeline(args.config, environment=args.env)
        pipeline.run()
    except Exception as e:
        logger.error(f"Silver pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
