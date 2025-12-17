	# Add internal rules
	gcloud compute firewall-rules create allow-internal-cluster \
	  --network=cdc-pipeline-vpc \
	  --action=ALLOW \
	  --rules=tcp,udp,icmp \
	  --source-ranges=10.0.0.0/24 \
	  --description="Allow all internal communication between cluster nodes" \
	  --priority=1000
	
	# add new config
	gcloud compute firewall-rules create allow-postgres-from-my-ip \
	  --network=cdc-pipeline-vpc \	
	  --allow=tcp:5432 \
	  --source-ranges=$(curl -s ifconfig.me)/32 \
	  --description="Allow PostgreSQL access from my current IP" \
	  --direction=INGRESS
	  
	# update config
	gcloud compute firewall-rules update allow-postgres-from-my-ip \
	  --source-ranges=$(curl -s ifconfig.me)/32