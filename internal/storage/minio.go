package storage

import (
	"context"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var minioClient *minio.Client
var bucketName string = "map-reduce-bucket"

func GetMinioClient() (*minio.Client, error) {
	if minioClient == nil {
		newClient, err := newMinioClient()

		if err != nil {
			return nil, err
		}

		minioClient = newClient
	}

	return minioClient, nil
}

func newMinioClient() (*minio.Client, error) {
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "minio:9000"
	}

	return minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	})
}

func ensureBucketExists(client *minio.Client, bucket string) error {
	ctx := context.Background()

	exists, err := client.BucketExists(ctx, bucket)

	if err != nil {
		return err
	}

	if !exists {
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return err
		}
	}

	return nil
}

func GetObject(objectName string) (*minio.Object, error) {
	client, err := GetMinioClient()
	ctx := context.Background()

	if err != nil {
		return nil, err
	}

	return client.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
}

func UploadData(objectName string, data string) error {
	client, err := GetMinioClient()
	ctx := context.Background()

	if err != nil {
		return err
	}

	if err := ensureBucketExists(client, bucketName); err != nil {
		return err
	}

	_, err = client.PutObject(ctx, bucketName, objectName, strings.NewReader(data), int64(len(data)), minio.PutObjectOptions{})

	return err
}
