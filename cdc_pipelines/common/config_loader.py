"""Configuration loading utilities."""

import os
from typing import Dict, Any
import yaml
from pathlib import Path
from dotenv import load_dotenv


class ConfigLoader:
    """Loads and manages configuration from YAML files and environment variables."""

    def __init__(self, base_dir: str = None, environment: str = None):
        """
        Initialize the ConfigLoader.

        Args:
            config_dir: Base directory for configuration files (default: "config")
            environment: Environment name (dev, prod, etc.) (default: from env var or "dev")
        """
        load_dotenv()
        self.environment = environment or os.getenv("ENVIRONMENT", "dev")
        
        if os.getenv("BASE_DIR"):
            self.base_dir = Path(os.getenv("BASE_DIR"))
        else:
            self.base_dir = Path(base_dir)

    def load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file.

        Returns:
            Configuration dictionary
        """
        config_file = self.base_dir / f'config_{self.environment}.yaml'

        if not Path(config_file).exists():
            raise FileNotFoundError(f"Configuration file not found: {config_file}")

        with open(config_file, "r") as f:
            config = yaml.safe_load(f)

        # Override with environment variables if present
        config = self._override_with_env(config)

        return config

    def _override_with_env(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Override configuration with environment variables.

        Args:
            config: Original configuration dictionary

        Returns:
            Updated configuration dictionary
        """
        # Kafka overrides
        if os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
            config.setdefault("kafka", {})["bootstrap_servers"] = os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS"
            )

        # Delta paths
        if os.getenv("DELTA_LAKE_PATH"):
            config.setdefault("delta", {})["base_path"] = os.getenv("DELTA_LAKE_PATH")
        if os.getenv("BRONZE_PATH"):
            config.setdefault("delta", {})["bronze_path"] = os.getenv("BRONZE_PATH")
        if os.getenv("SILVER_PATH"):
            config.setdefault("delta", {})["silver_path"] = os.getenv("SILVER_PATH")
        if os.getenv("GOLD_PATH"):
            config.setdefault("delta", {})["gold_path"] = os.getenv("GOLD_PATH")

        # Checkpoint paths
        if os.getenv("CHECKPOINT_LOCATION"):
            config.setdefault("checkpoints", {})["base_path"] = os.getenv(
                "CHECKPOINT_LOCATION"
            )

        # Spark overrides
        if os.getenv("SPARK_MASTER"):
            config.setdefault("spark", {})["master"] = os.getenv("SPARK_MASTER")

        return config

    def get_table_config(self, table_name: str) -> Dict[str, Any]:
        """
        Get configuration for a specific table.

        Args:
            table_name: Name of the table

        Returns:
            Table configuration dictionary
        """
        tables_config = self.load_tables_config()
        table_config = tables_config.get("tables", {}).get(table_name)

        if not table_config:
            raise ValueError(f"Configuration not found for table: {table_name}")

        return table_config


def load_config(config_dir: str = None, environment: str = None) -> Dict[str, Any]:
    """
    Load configuration from YAML file.

    Args:
        config_dir: Base directory for configuration files
        environment: Environment name (dev, prod, etc.)

    Returns:
        Configuration dictionary
    """
    loader = ConfigLoader(config_dir, environment)
    return loader.load_config()
