"""Gold layer pipeline for building aggregated analytics tables."""

from cdc_pipelines.common.config_loader import ConfigLoader
from cdc_pipelines.common.spark_session import get_spark_session
from cdc_pipelines.common.logger import PipelineLogger
from cdc_pipelines.pipelines.gold.jobs import (
    build_product_performance_daily,
    build_customer_order_summary,
    build_order_hourly_patterns
)

PIPELINE_LAYER = "gold"


class GoldPipeline:
    """
    Orchestrates the gold layer pipeline.
    Builds aggregated analytics tables from silver layer data.
    """

    def __init__(self, config_path: str, environment: str = "dev"):
        """
        Initialize gold pipeline.

        Args:
            config_path: Path to configuration file
            environment: Environment name (dev, prod, etc.)
        """
        config_loader = ConfigLoader(config_path, environment=environment)
        self.config = config_loader.load_config()

        self.logger = PipelineLogger(__name__, PIPELINE_LAYER, PIPELINE_LAYER)
        self.logger.info(f"Starting {PIPELINE_LAYER} layer pipeline")

        self.spark = get_spark_session(self.config, name="gold")
        
        self.logger.info("Initialized GoldPipeline")

    def run(self):
        """
        Run the gold pipeline to build all analytics tables.
        This is a batch process that runs periodically.
        """
        self.logger.info("Starting gold pipeline execution...")
        
        try:
            # Build all gold tables using modular processors
            build_product_performance_daily(self.spark, self.config)
            build_customer_order_summary(self.spark, self.config)
            build_order_hourly_patterns(self.spark, self.config)
            
            self.logger.info("Gold pipeline execution completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in gold pipeline: {str(e)}", exc_info=True)
            raise

    def stop(self):
        """Stop the pipeline and cleanup resources."""
        self.logger.info("Stopping gold pipeline...")
        self.spark.stop()
        self.logger.info("Gold pipeline stopped")
