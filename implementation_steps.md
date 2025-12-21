# Project Implementation Steps

### Phase 1: GCP Infrastructure Setup

- [x] Create GCP project and enable billing with $300 credit
- [x] Enable required APIs (Compute Engine, GCS Buckets)
- [x] Create VPC network and subnet (10.0.0.0/24)
- [x] Configure firewall rules for internal communication
- [ ] Configure firewall rules for external access (SSH, web UIs)
- [x] Create GCS bucket for Delta Lake
- [x] Create service account with Pub/Sub and GCS permissions
- [x] Provision 3 x e2-standard-2 VMs with static internal IPs
- [ ] Set up Kafka 

### Phase 2: Base Software Installation (All Nodes)

- [x] Install Java 17 (OpenJDK) on nodes 2, 3
- [x] Install Python 3.10+ and pip on all nodes
- [x] Configure SSH key-based authentication between nodes
- [x] Set up `/etc/hosts` for hostname resolution between nodes
- [x] Install Google Cloud SDK on all nodes
- [x] Configure service account credentials on all nodes

### Phase 3: Node 1 - Source Layer Setup

- [x] Install PostgreSQL 15
- [x] Configure `postgresql.conf` for logical replication (wal_level=logical)
- [x] Create database user with replication permissions
- [x] Create OLTP database schema (orders, products tables)
- [x] Download and configure Debezium Server
- [x] Configure Debezium PostgreSQL connector
- [ ] Configure Debezium Kafka sink
- [x] Install Jupyter Notebook
- [x] Create Python virtual environment for Data Generator
- [x] Develop Data Generator application
- [x] Test CDC flow: Generator → PostgreSQL → Debezium → Kafka

### Phase 4: Node 2 & 3 - Spark Cluster Setup

- [x] Download and extract Apache Spark 4 on Node 2 and Node 3
- [x] Configure Spark environment variables on both nodes
- [x] Configure `spark-env.sh` on Node 2 (Master settings)
- [x] Configure `spark-env.sh` on Node 3 (Worker settings)
- [x] Configure `spark-defaults.conf` with Delta Lake and GCS dependencies
- [ ] Download required JARs (Delta Lake, GCS connector, Kafka connector)
- [x] Start Spark Master on Node 2
- [x] Start Spark Worker on Node 2, register to Master
- [x] Start Spark Worker on Node 3, register to Master
- [x] Verify cluster via Spark Web UI (port 8080)
- [x] Run test job to verify distributed execution

### Phase 5: Streaming Pipeline Development

- [ ] Develop Spark Structured Streaming job (Kafka source)
- [ ] Implement Debezium JSON envelope parsing
- [ ] Implement CDC operation handling (insert/update/delete)
- [ ] Configure Delta Lake sink for bronze layer
- [ ] Test streaming job with sample CDC events
- [ ] Deploy streaming job to cluster
- [ ] Verify data landing in GCS bronze layer

### Phase 6: Medallion Architecture

- [ ] Develop bronze → silver transformation job (deduplication, cleaning)
- [ ] Implement Type 2 SCD logic for silver layer
- [ ] Develop silver → gold aggregation jobs
- [ ] Schedule batch jobs (cron or Spark job scheduler)
- [ ] Verify medallion data flow end-to-end

### Phase 7: Query Layer Setup

- [ ] Configure Spark Thrift Server on Node 2
- [ ] Create external Delta tables in Spark catalog
- [ ] Start Thrift Server
- [ ] Test JDBC connectivity to Thrift Server
- [ ] Verify SQL queries against Delta tables

### Phase 8: Node 3 - Visualization Setup

- [ ] Install PostgreSQL for Metabase metadata
- [ ] Create Metabase database and user
- [ ] Download and install Metabase
- [ ] Configure Metabase to use local PostgreSQL
- [ ] Start Metabase service
- [ ] Connect Metabase to Spark Thrift Server via JDBC
- [ ] Create data source pointing to gold layer tables

### Phase 9: Dashboard Development

- [ ] Design dashboard requirements (KPIs, charts)
- [ ] Create orders real-time monitoring dashboard
- [ ] Create sales trends dashboard
- [ ] Create customer analytics dashboard
- [ ] Create product performance dashboard
- [ ] Configure dashboard auto-refresh intervals

### Phase 10: Testing & Optimization

- [ ] Run end-to-end pipeline test
- [ ] Verify data latency (source to dashboard)
- [ ] Monitor Spark job performance
- [ ] Tune Spark streaming batch intervals
- [ ] Tune Delta Lake compaction settings
- [ ] Document the architecture and configurations

### Phase 11: Cost Optimization

- [ ] Create VM start/stop scripts
- [ ] Set up scheduled shutdown (Cloud Scheduler or cron)
- [ ] Consider converting to Spot VMs for development
- [ ] Monitor and review GCP billing
