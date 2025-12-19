	# Add internal rules
gcloud compute firewall-rules create allow-internal-cluster \
	--network=cdc-pipeline-vpc \
	--action=ALLOW \
	--rules=tcp,udp,icmp \
	--source-ranges=10.0.0.0/24 \
	--description="Allow all internal communication between cluster nodes" \
	--priority=1000

MY_IP=$(curl -4 ifconfig.me)

# add new config
gcloud compute firewall-rules create allow-from-my-ip \
  --network=cdc-pipeline-vpc \
  --action=allow \
  --rules=tcp:22,tcp:3000,tcp:5432,tcp:5433,tcp:7077,tcp:8080,tcp:8081,tcp:8083,tcp:8888,tcp:10000 \
  --source-ranges="${MY_IP}/32" \
  --description="Allow access from my current IP (${MY_IP})" \
  --priority=1000 \
  --direction=INGRESS
	
# update config
gcloud compute firewall-rules update allow-from-my-ip \
	--source-ranges=$(curl -4 ifconfig.me)/32