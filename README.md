# GoMR

GoMR is a local Kubernetes-backed MapReduce runner for Go workers. You write a Go file with a mapper and reducer, then run one CLI command to build the worker image, start the runtime, upload input, and run the job.

## What It Does

GoMR takes:

- a local input file
- a Go worker file
- worker, mapper, and reducer counts

Then it:

1. Starts MinIO in Kubernetes.
2. Uploads the input file to MinIO.
3. Builds the master Docker image.
4. Builds a worker Docker image from your Go worker file.
5. Deploys the master and worker pods.
6. Runs map tasks, then reduce tasks.
7. Writes final output back to MinIO.

## Architecture

The system has three main parts:

- CLI: prepares the run, builds images, uploads input, and applies Kubernetes manifests.
- Master: splits input, schedules map/reduce tasks, and tracks worker completion.
- Workers: run the user mapper/reducer code and read/write data through MinIO.

MinIO is the shared storage layer. The master and workers exchange large data through MinIO instead of sending it over gRPC. gRPC is used for worker registration, task assignment, and completion reporting.

## Tech Stack

- Go
- Docker
- Kubernetes
- MinIO
- gRPC
- Protocol Buffers

## Prerequisites

Install and run:

- Go
- Docker
- `kubectl`
- `kind` or `minikube`

The CLI can load locally built images into `kind` and `minikube` clusters.

## Run The Example

Create an input file:

```sh
printf 'one two one\nthree two one\n' > /tmp/gomr-input.txt
```

Run the word count worker:

```sh
go run ./cmd/gomr start \
  --input /tmp/gomr-input.txt \
  --workers 2 \
  --mappers 2 \
  --reducers 2 \
  examples/wordcount/worker.go
```

The master logs should eventually show:

```txt
All workers connected
All map tasks completed!
Map reduce completed
```

## Output

Output is written to MinIO under:

```txt
map-reduce-bucket/outputs/
```

To inspect it locally:

```sh
kubectl port-forward --address 127.0.0.1 service/minio 39000:9000
```

Then connect with any S3-compatible client:

```txt
endpoint:   http://127.0.0.1:39000
access key: minioadmin
secret key: minioadmin
bucket:     map-reduce-bucket
prefix:     outputs/
```

## Writing A Worker

A worker is a Go file that imports `gomr/pkg/gomr`, defines a mapper and reducer, then calls `gomr.Run`.

```go
package main

import "gomr/pkg/gomr"

func mapper(ctx gomr.Context, chunk string) {
    ctx.Emit("key", "1")
}

func reducer(ctx gomr.Context, key string, values []int) {
    ctx.Emit(key, "result")
}

func main() {
    gomr.Run(&gomr.MapReduceConfig{
        Mapper:  mapper,
        Reducer: reducer,
    })
}
```

See `examples/wordcount/worker.go` for a working example.

## Development

Run checks:

```sh
go test ./...
```

Regenerate protobuf files:

```sh
make proto
```

Build images manually:

```sh
docker build -f Dockerfile.master -t mr-master:latest .
docker build -f Dockerfile.worker --build-arg WORKER_FILE=examples/wordcount/worker.go -t mr-worker:latest .
```

## Cleanup

```sh
kubectl delete deployment master worker minio
kubectl delete service master minio
kubectl delete pvc minio-pvc
```
