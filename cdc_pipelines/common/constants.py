"""Constants used across the pipeline."""

# CDC Operation Types
CDC_OP_CREATE = "c"
CDC_OP_READ = "r"
CDC_OP_UPDATE = "u"
CDC_OP_DELETE = "d"

CDC_OPERATIONS = {
    CDC_OP_CREATE: "INSERT",
    CDC_OP_READ: "READ",
    CDC_OP_UPDATE: "UPDATE",
    CDC_OP_DELETE: "DELETE",
}

# Column Names
COL_KAFKA_TOPIC = "kafka_topic"
COL_KAFKA_PARTITION = "kafka_partition"
COL_KAFKA_OFFSET = "kafka_offset"
COL_KAFKA_TIMESTAMP = "kafka_timestamp"
COL_INGESTION_TIMESTAMP = "ingestion_timestamp"
COL_CDC_OPERATION = "cdc_operation"
COL_CDC_TIMESTAMP = "cdc_timestamp"

# SCD2 Columns
COL_VALID_FROM = "valid_from"
COL_VALID_TO = "valid_to"
COL_IS_CURRENT = "is_current"
COL_ROW_HASH = "row_hash"

# Default Values
DEFAULT_VALID_TO = "9999-12-31 23:59:59"
DEFAULT_IS_CURRENT = True

# Kafka Configuration
KAFKA_STARTING_OFFSETS = "earliest"
KAFKA_FAIL_ON_DATA_LOSS = "false"

# Delta Lake Options
DELTA_MERGE_SCHEMA = "true"
DELTA_AUTO_OPTIMIZE = "true"

# Checkpoint Configuration
CHECKPOINT_SUFFIX_BRONZE = "bronze"
CHECKPOINT_SUFFIX_SILVER = "silver"
CHECKPOINT_SUFFIX_GOLD = "gold"

# Layer Names
LAYER_BRONZE = "bronze"
LAYER_SILVER = "silver"
LAYER_GOLD = "gold"

# Table Prefixes
TABLE_PREFIX_BRONZE = "bronze"
TABLE_PREFIX_SILVER = "silver"
TABLE_PREFIX_GOLD = "gold"

# Date/Time Formats
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

# Pipeline States
STATE_STARTING = "STARTING"
STATE_RUNNING = "RUNNING"
STATE_STOPPED = "STOPPED"
STATE_FAILED = "FAILED"

# Data Quality
DQ_CHECK_NULLS = "check_nulls"
DQ_CHECK_DUPLICATES = "check_duplicates"
DQ_CHECK_REFERENTIAL_INTEGRITY = "check_referential_integrity"
DQ_CHECK_SCHEMA = "check_schema"

# Monitoring
METRIC_RECORDS_PROCESSED = "records_processed"
METRIC_RECORDS_FAILED = "records_failed"
METRIC_PROCESSING_TIME = "processing_time_seconds"
METRIC_LAG = "lag_seconds"
