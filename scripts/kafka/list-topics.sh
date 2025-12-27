# list topics
/opt/kafka/bin/kafka-topics.sh --bootstrap-server 10.0.0.2:9092 --list

# delete topics 
/opt/kafka/bin/kafka-topics.sh --bootstrap-server 10.0.0.2:9092 --delete --topic cdc-pipeline.instacart.*
