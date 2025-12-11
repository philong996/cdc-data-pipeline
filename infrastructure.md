# 3-Node Cluster Service Distribution

## Node 1: Source & Ingestion

| Service | RAM | Ports |
|---------|-----|-------|
| PostgreSQL (OLTP source) | 3 GB | 5432 |
| Debezium Server | 1.5 GB | 8083 |
| Data Generator (Python) | 512 MB | - |
| Jupyter Notebook | 2 GB | 8888 |
| OS + Buffer | ~9 GB | - |

## Node 2: Spark Master + Worker

| Service | RAM | Ports |
|---------|-----|-------|
| Spark Master | 1 GB | 7077, 8080 |
| Spark Worker | 12 GB | 8081 |
| Spark Thrift Server | 2 GB | 10000 |
| OS + Buffer | ~1 GB | - |

## Node 3: Spark Worker + Visualization

| Service | RAM | Ports |
|---------|-----|-------|
| Spark Worker | 12 GB | 8081 |
| Metabase | 2 GB | 3000 |
| PostgreSQL (Metabase metadata) | 1 GB | 5433 |
| OS + Buffer | ~1 GB | - |

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
