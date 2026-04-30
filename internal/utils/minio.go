package utils

import (
	"context"
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
	return minio.New("minio:9000", &minio.Options{
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
		client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
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

	_, err = client.PutObject(ctx, bucketName, objectName, strings.NewReader(data), int64(len(data)), minio.PutObjectOptions{})

	return err
}
