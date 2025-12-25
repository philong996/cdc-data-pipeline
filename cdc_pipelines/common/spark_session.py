"""Spark Session initialization module."""

from typing import Dict, Any
from pyspark.sql import SparkSession
from cdc_pipelines.common.logger import get_logger

logger = get_logger(__name__)


class SparkSessionManager:
    """Manages Spark Session creation and configuration."""

    _instance = None

    @classmethod
    def get_or_create(cls, config: Dict[str, Any]) -> SparkSession:
        """
        Get or create a Spark session with the provided configuration.

        Args:
            config: Configuration dictionary containing spark settings

        Returns:
            SparkSession instance
        """
        if cls._instance is None:
            logger.info("Creating new Spark session")
            cls._instance = cls._create_session(config)
        else:
            logger.info("Reusing existing Spark session")

        return cls._instance

    @classmethod
    def _create_session(cls, config: Dict[str, Any]) -> SparkSession:
        """
        Create a new Spark session.

        Args:
            config: Configuration dictionary

        Returns:
            SparkSession instance
        """
        spark_config = config.get("spark", {})
        app_name = spark_config.get("app_name", "cdc-streaming-pipeline")
        master = spark_config.get("master", "local[*]")
        spark_conf = spark_config.get("config", {})

        builder = SparkSession.builder.appName(app_name).master(master)

        # Apply Spark configurations
        for key, value in spark_conf.items():
            if key == "spark.jars.packages" and isinstance(value, list):
                value = ",".join(value)
            builder = builder.config(key, value)

        spark = builder.getOrCreate()

        # Set log level
        log_level = spark_config.get("log_level", "INFO")
        spark.sparkContext.setLogLevel(log_level)

        logger.info(f"Spark session created: {app_name}")
        logger.info(f"Spark version: {spark.version}")

        return spark

    @classmethod
    def stop(cls):
        """Stop the Spark session."""
        if cls._instance is not None:
            logger.info("Stopping Spark session")
            cls._instance.stop()
            cls._instance = None


def get_spark_session(config: Dict[str, Any]) -> SparkSession:
    """
    Get or create a Spark session.

    Args:
        config: Configuration dictionary

    Returns:
        SparkSession instance
    """
    return SparkSessionManager.get_or_create(config)
