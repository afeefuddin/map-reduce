package cli

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const defaultBucket = "map-reduce-bucket"

func uploadInput(ctx context.Context, data *GomrData) error {
	port, err := freeLocalPort()
	if err != nil {
		return err
	}

	pfCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cmd := exec.CommandContext(pfCtx, "kubectl", "port-forward", "--address", "127.0.0.1", "service/minio", fmt.Sprintf("%d:9000", port))
	cmd.Stdout = io.Discard
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start minio port-forward: %w", err)
	}
	defer func() {
		cancel()
		_ = cmd.Wait()
	}()

	if err := waitForTCP(ctx, fmt.Sprintf("127.0.0.1:%d", port), 20*time.Second); err != nil {
		return err
	}

	client, err := minio.New(fmt.Sprintf("127.0.0.1:%d", port), &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	})
	if err != nil {
		return fmt.Errorf("connect to minio: %w", err)
	}

	if err := ensureBucket(ctx, client, defaultBucket, 60*time.Second); err != nil {
		return err
	}
	for _, prefix := range []string{"splitted-input/", "workers/", "outputs/"} {
		if err := clearPrefix(ctx, client, defaultBucket, prefix); err != nil {
			return err
		}
	}

	log.Printf("Uploading %s to minio://%s/%s", data.inputPath, defaultBucket, data.inputObject)
	if _, err := client.FPutObject(ctx, defaultBucket, data.inputObject, data.inputPath, minio.PutObjectOptions{}); err != nil {
		return fmt.Errorf("upload input: %w", err)
	}

	return nil
}

func clearPrefix(ctx context.Context, client *minio.Client, bucket, prefix string) error {
	objects := make(chan minio.ObjectInfo)
	go func() {
		defer close(objects)
		for object := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			objects <- object
		}
	}()

	for removeErr := range client.RemoveObjects(ctx, bucket, objects, minio.RemoveObjectsOptions{}) {
		if removeErr.Err != nil {
			return fmt.Errorf("clear minio prefix %q: %w", prefix, removeErr.Err)
		}
	}

	return nil
}

func ensureBucket(ctx context.Context, client *minio.Client, bucket string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		exists, err := client.BucketExists(ctx, bucket)
		if err == nil {
			if exists {
				return nil
			}
			if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
				lastErr = err
			} else {
				return nil
			}
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}

	return fmt.Errorf("minio bucket %q was not ready: %w", bucket, lastErr)
}
