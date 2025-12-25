# 3-Node Cluster Service Distribution

## Node 1: Source & Ingestion

| Service | Ports |
|---------|-------|
| PostgreSQL (OLTP source) | 5432 |
| Debezium Server | 8083 |
| Zookeeper | 2181 |
| Kafka | 9092 |
| Data Generator (Python) | - |
| Jupyter labs | 8888 |

## Node 2: Spark Master + Worker

| Service | Ports |
|---------|-------|
| Spark Master | 7077, 8080 |
| Spark Worker | 8081 |
| Spark Thrift Server | 10000 |

## Node 3: Spark Worker + Visualization

| Service | Ports |
|---------|-------|
| Spark Worker | 8081 |
| Metabase | 3000 |
| PostgreSQL (Metabase metadata) | 5433 |

## GCP Managed Services

| Service | Purpose |
|---------|---------|
| GCS Bucket | Delta Lake storage |
