# 3-Node Cluster Service Distribution

## Node 1: Source & Ingestion

| Service | Ports |
|---------|-------|
| PostgreSQL (OLTP source) | 5432 |
| Debezium Server | 8083 |
| Data Generator (Python) | - |
| Jupyter Notebook | 8888 |

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
| Pub/Sub | CDC message streaming |
| GCS Bucket | Delta Lake storage |


# Startup Order by Node

- **Node 1: Source & Ingestion**
  - Order 1: PostgreSQL (OLTP) - No dependencies
  - Order 6: Debezium Server - Depends on: PostgreSQL (same node), Pub/Sub (GCP)
  - Order 9: Jupyter Notebook - No dependencies
  - Order 10: Data Generator - Depends on: PostgreSQL (same node)

- **Node 2: Spark Master + Worker**
  - Order 2: Spark Master - No dependencies
  - Order 3: Spark Worker (Node 2) - Depends on: Spark Master (same node)
  - Order 7: Spark Thrift Server - Depends on: Spark Master (same node)

- **Node 3: Spark Worker + Visualization**
  - Order 4: PostgreSQL (Metabase) - No dependencies
  - Order 5: Spark Worker (Node 3) - Depends on: **Spark Master (Node 2) ← CROSS-NODE DEPENDENCY**
  - Order 8: Metabase - Depends on: PostgreSQL Metabase (same node)

## Critical Startup Sequence

1. **Start Node 2 first** - Spark Master must be running and accessible on port 7077
2. **Then start Node 1 and Node 3** - Can run in parallel after Spark Master is ready
3. **Note**: Node 3's Spark Worker will fail to start if Node 2's Spark Master is not reachable
