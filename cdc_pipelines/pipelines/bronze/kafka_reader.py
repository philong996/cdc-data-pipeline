"""Kafka reader for streaming CDC events."""

from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp
from cdc_pipelines.common.logger import get_logger
from cdc_pipelines.common.constants import KAFKA_STARTING_OFFSETS, KAFKA_FAIL_ON_DATA_LOSS

logger = get_logger(__name__)


class KafkaReader:
    """Reads streaming data from Kafka topics."""

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize Kafka reader.

        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.kafka_config = config.get("kafka", {})

    def read_stream(self, topics: list = None, group_id: str = None) -> DataFrame:
        """
        Read streaming data from Kafka topics.

        Args:
            topics: List of Kafka topics to subscribe to
            group_id: Kafka consumer group ID (optional, defaults to config value)

        Returns:
            Streaming DataFrame
        """
        topics = topics or self.kafka_config.get("topics", [])
        bootstrap_servers = self.kafka_config.get("bootstrap_servers")

        if not bootstrap_servers:
            raise ValueError("Kafka bootstrap servers not configured")

        if not topics:
            raise ValueError("No Kafka topics specified")

        # Use provided group_id or fall back to config
        consumer_group_id = group_id or self.kafka_config.get("group_id", "cdc-streaming")

        logger.info(f"Reading from Kafka topics: {topics}")
        logger.info(f"Bootstrap servers: {bootstrap_servers}")
        logger.info(f"Consumer group ID: {consumer_group_id}")

        kafka_options = {
            "kafka.bootstrap.servers": bootstrap_servers,
            "subscribe": ",".join(topics),
            "startingOffsets": KAFKA_STARTING_OFFSETS,
            "failOnDataLoss": KAFKA_FAIL_ON_DATA_LOSS,
            "groupIdPrefix": consumer_group_id,
        }

        # Add security configuration if present
        security_protocol = self.kafka_config.get("security_protocol")
        if security_protocol and security_protocol != "PLAINTEXT":
            kafka_options["kafka.security.protocol"] = security_protocol

            sasl_mechanism = self.kafka_config.get("sasl_mechanism")
            if sasl_mechanism:
                kafka_options["kafka.sasl.mechanism"] = sasl_mechanism

        # Read from Kafka
        df = self.spark.readStream.format("kafka").options(**kafka_options).load()
        
        # keep only necessary columns
        df = df.selectExpr("CAST(value AS STRING) as json_payload",
                "topic", 
                "partition", 
                "offset",
                "timestamp as kafka_timestamp"
            ).withColumn("ingestion_timestamp", current_timestamp())


        logger.info("Successfully connected to Kafka stream")

        return df

    def read_batch(self, topics: list = None, start_offset: str = "earliest") -> DataFrame:
        """
        Read batch data from Kafka topics (for testing/backfill).

        Args:
            topics: List of Kafka topics to subscribe to
            start_offset: Starting offset (earliest or latest)

        Returns:
            Batch DataFrame
        """
        topics = topics or self.kafka_config.get("topics", [])
        bootstrap_servers = self.kafka_config.get("bootstrap_servers")

        logger.info(f"Reading batch from Kafka topics: {topics}")

        kafka_options = {
            "kafka.bootstrap.servers": bootstrap_servers,
            "subscribe": ",".join(topics),
            "startingOffsets": start_offset,
        }

        df = self.spark.read.format("kafka").options(**kafka_options).load()

        logger.info(f"Read {df.count()} records from Kafka")

        return df
