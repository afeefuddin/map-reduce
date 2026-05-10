package cli

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"strings"
)

func applyMinIO(ctx context.Context) error {
	log.Println("Applying MinIO resources")
	for _, path := range []string{
		"pkg/deployment/infra/minio/pvc.yaml",
		"pkg/deployment/infra/minio/deployment.yaml",
		"pkg/deployment/infra/minio/service.yaml",
	} {
		if err := run(ctx, "kubectl", "apply", "-f", path); err != nil {
			return err
		}
	}

	return run(ctx, "kubectl", "wait", "--for=condition=available", "deployment/minio", "--timeout=180s")
}

func buildImages(ctx context.Context, data *GomrData) error {
	log.Printf("Building master image %s", data.masterImage)
	if err := run(ctx, "docker", "build", "-f", "Dockerfile.master", "-t", data.masterImage, "."); err != nil {
		return err
	}

	log.Printf("Building worker image %s from %s", data.workerImage, data.workerBuildPath)
	return run(ctx, "docker", "build", "-f", "Dockerfile.worker", "--build-arg", "WORKER_FILE="+data.workerBuildPath, "-t", data.workerImage, ".")
}

func loadImagesIntoLocalCluster(ctx context.Context, data *GomrData) error {
	currentContext, err := commandOutput(ctx, "kubectl", "config", "current-context")
	if err != nil {
		return err
	}

	currentContext = strings.TrimSpace(currentContext)
	switch {
	case strings.HasPrefix(currentContext, "kind-"):
		if err := requireCommand("kind"); err != nil {
			log.Printf("Skipping kind image load: %s", err)
			return nil
		}
		cluster := strings.TrimPrefix(currentContext, "kind-")
		log.Printf("Loading images into kind cluster %s", cluster)
		if err := run(ctx, "kind", "load", "docker-image", data.masterImage, "--name", cluster); err != nil {
			return err
		}
		return run(ctx, "kind", "load", "docker-image", data.workerImage, "--name", cluster)
	case strings.Contains(currentContext, "minikube"):
		if err := requireCommand("minikube"); err != nil {
			log.Printf("Skipping minikube image load: %s", err)
			return nil
		}
		log.Println("Loading images into minikube")
		if err := run(ctx, "minikube", "image", "load", data.masterImage); err != nil {
			return err
		}
		return run(ctx, "minikube", "image", "load", data.workerImage)
	default:
		log.Printf("Skipping cluster image load for kubectl context %q", currentContext)
		return nil
	}
}

func deployMasterAndWorkers(ctx context.Context, data *GomrData) error {
	if err := stopExistingWorkers(ctx); err != nil {
		return err
	}

	log.Println("Applying master service and deployment")
	if err := run(ctx, "kubectl", "apply", "-f", "pkg/deployment/master/service.yaml"); err != nil {
		return err
	}
	if err := applyMasterDeployment(ctx, data); err != nil {
		return err
	}
	if err := run(ctx, "kubectl", "rollout", "status", "deployment/master", "--timeout=180s"); err != nil {
		return err
	}

	log.Println("Applying worker deployment")
	if err := applyWorkerDeployment(ctx, data); err != nil {
		return err
	}
	return run(ctx, "kubectl", "rollout", "status", "deployment/worker", "--timeout=180s")
}

func stopExistingWorkers(ctx context.Context) error {
	if err := exec.CommandContext(ctx, "kubectl", "get", "deployment/worker").Run(); err != nil {
		return nil
	}

	log.Println("Scaling existing workers down before restarting master")
	if err := run(ctx, "kubectl", "scale", "deployment/worker", "--replicas=0"); err != nil {
		return err
	}
	return run(ctx, "kubectl", "rollout", "status", "deployment/worker", "--timeout=180s")
}

func applyMasterDeployment(ctx context.Context, data *GomrData) error {
	if err := run(ctx, "kubectl", "apply", "-f", "pkg/deployment/master/deployment.yaml"); err != nil {
		return err
	}
	if err := run(ctx, "kubectl", "set", "image", "deployment/master", "master="+data.masterImage); err != nil {
		return err
	}
	return run(ctx, "kubectl", "set", "env", "deployment/master",
		"INPUT_PATH="+data.inputObject,
		"WORKERS_COUNT="+fmt.Sprint(data.workerCount),
		"MAPPERS_COUNT="+fmt.Sprint(data.mapperWorkersCount),
		"REDUCERS_COUNT="+fmt.Sprint(data.reducerWorkersCount),
		"GOMR_ROLLOUT_ID="+data.rolloutID,
	)
}

func applyWorkerDeployment(ctx context.Context, data *GomrData) error {
	if err := run(ctx, "kubectl", "apply", "-f", "pkg/deployment/worker/deployment.yaml"); err != nil {
		return err
	}
	if err := run(ctx, "kubectl", "set", "image", "deployment/worker", "worker="+data.workerImage); err != nil {
		return err
	}
	if err := run(ctx, "kubectl", "set", "env", "deployment/worker", "GOMR_ROLLOUT_ID="+data.rolloutID); err != nil {
		return err
	}
	return run(ctx, "kubectl", "scale", "deployment/worker", "--replicas="+fmt.Sprint(data.workerCount))
}
