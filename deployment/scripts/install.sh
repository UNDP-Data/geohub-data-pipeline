# Source the .env file located in the same directory as the script
. ./.env

# Rest of the script
kubectl apply -f ../yaml/ingest-namespace.yaml
envsubst < ../yaml/ingest-deployment.yaml | kubectl apply -f -
unset AZURE_ACCESS_KEY ACCOUNT_NAME CREDENTIAL_STRING CONTAINER_NAME ACCOUNT_URL CONNECTION_STRING
kubectl apply -f ../yaml/ingest-service.yaml

kubectl apply -f ../yaml/ingest-ingress.yaml
wait_time=60
echo "going to wait ${wait_time} seconds to check the ingress"
sleep ${wait_time}
kubectl get ingress -n data