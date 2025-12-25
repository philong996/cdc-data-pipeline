CREATE ROLE debezium_user WITH REPLICATION LOGIN PASSWORD 'debezium_user';
GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
GRANT USAGE ON SCHEMA instacart TO debezium_user;
GRANT SELECT ON ALL TABLES IN SCHEMA instacart TO debezium_user;

CREATE PUBLICATION dbz_publication FOR ALL TABLES;

SELECT schemaname, tablename, relreplident 
FROM pg_catalog.pg_tables t
JOIN pg_catalog.pg_class c ON t.tablename = c.relname
WHERE schemaname = 'public';
