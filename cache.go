package minioproto

import (
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"log"
	"net/url"
	"strings"
)

// Cache is a basic wrapper around minio.Client with support for storing Protobuf, JSON or CSV files.
type Cache struct {
	ctx        context.Context
	client     *minio.Client
	bucketName string
	logger     *zap.Logger
}

// NewFromURL creates a new instance using a connection url:
// > http(s)://<user>:<password>@<host>/<bucket>?token=<token>
func NewFromURL(ctx context.Context, logger *zap.Logger, connectionURL string) (*Cache, error) {
	config, err := url.Parse(connectionURL)
	if nil != err {
		err := errors.New("Failed to parse connection url")
		logger.Error(err.Error())
		return nil, err
	}

	useSSL := config.Scheme == "https"
	address := config.Host
	accessKey := config.User.Username()
	accessSecret, _ := config.User.Password()
	bucketName := config.Path
	if strings.HasSuffix(bucketName, "/") {
		bucketName = bucketName[1:]
	}
	token := config.Query().Get("token")

	return New(ctx, logger, bucketName, address, accessKey, accessSecret, token, useSSL)
}

// New creates a Cache instance using the given configuration
func New(ctx context.Context, logger *zap.Logger, bucketName, address, accessKey, accessSecret, token string, useSSL bool) (*Cache, error) {
	logger.Info(fmt.Sprintf("Connecting to minio server address=%v with bucket=%v", address, bucketName))

	// Configure the client connection
	creds := credentials.NewStaticV4(accessKey, accessSecret, token)
	options := minio.Options{
		Creds:  creds,
		Secure: useSSL,
	}
	client, err := minio.New(address, &options)
	if err != nil {
		err = errors.Wrap(err, "Failed to authenticate to minio server")
		logger.Error(err.Error())
		return nil, err
	}

	// Initialize the bucket
	logger.Info(fmt.Sprintf("Initalizing bucket=%v", bucketName))
	err = client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := client.BucketExists(ctx, bucketName)
		if errBucketExists == nil && exists {
			log.Printf("We already own %s\n", bucketName)
			logger.Info(fmt.Sprintf("Bucket already exists, bucket=%v", bucketName))
		} else {
			err = errors.Wrap(err, fmt.Sprintf("Failed to create bucket %v", bucketName))
			logger.Error(err.Error())
			return nil, err
		}
	} else {
		logger.Info(fmt.Sprintf("Bucket created=%v", bucketName))
	}

	output := &Cache{
		ctx:        ctx,
		client:     client,
		logger:     logger,
		bucketName: bucketName,
	}
	return output, nil
}
