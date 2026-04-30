docker build -t mr-master:latest -f Dockerfile.master .
docker build -t mr-worker:latest -f Dockerfile.worker .
kind load docker-image mr-master:latest
kind load docker-image mr-worker:latest