kubectl delete -f ../yaml/ingest-environment.yaml
kubectl delete secret ingest-secrets --ignore-not-found -n data
