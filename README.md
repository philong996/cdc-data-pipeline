# CDC DATA PIPELINE ON Google Cloud

## Stack 
PostgreSQL → Debezium Server → Kafka → Spark → Delta Lake → Metabase


## Infrastructure
- Node 1 (Client/Coordination): PostgreSQL, Debezium Server, Kafka, Jupyter
- Node 2 (Spark Master): Spark Master/Worker, Thrift Server
- Node 3 (Spark Worker): Spark Worker, Metabase
- Storage: GCS Buckets for Delta Lake tables


## Medalion Architecture
- Bronze (Streaming):
    - Kafka → Spark Streaming → Delta Lake
    - Preserves complete CDC envelope (before/after/source)

- Silver (Streaming):
    - Reads new Bronze records since last LSN
    - MERGE for SCD Type 2 with smart change detection
    - Tracks progress with watermark table

- Gold (Batch):
    - Scheduled Spark job in batch mode
    - Pre-aggregated metrics from Silver
    - Denormalized tables for dashboards