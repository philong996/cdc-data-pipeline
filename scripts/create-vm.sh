
# Spark cluster VM creation script
gcloud compute instances create node-2 \
  --zone=asia-southeast1-b \
  --machine-type=e2-standard-2 \
  --network=cdc-pipeline-vpc \
  --subnet=cdc-pipeline-subnet \
  --network-tier=STANDARD \
  --address=external-ip-node-2 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=20GB \
  --boot-disk-type=pd-standard

gcloud compute instances create node-3 \
  --zone=asia-southeast1-b \
  --machine-type=e2-standard-2 \
  --network=cdc-pipeline-vpc \
  --subnet=cdc-pipeline-subnet \
  --network-tier=STANDARD \
  --address=external-ip-node-3 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=20GB \
  --boot-disk-type=pd-standard  