package minioproto

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"io/ioutil"
	"log"
	"net/url"
	"path/filepath"
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

//
// Exists checks
//

// PROTOExists checks if PROTO file exists in minio
func (cache *Cache) PROTOExists(path string, opts minio.StatObjectOptions) (*minio.ObjectInfo, error) {
	path = pathFix(path, jsonContentType)
	return cache.DataExists(path, opts)
}

// JSONExists checks if JSON file exists in minio
func (cache *Cache) JSONExists(path string, opts minio.StatObjectOptions) (*minio.ObjectInfo, error) {
	path = pathFix(path, jsonContentType)
	return cache.DataExists(path, opts)
}

// CSVExists checks if CSV file exists in minio
func (cache *Cache) CSVExists(path string, opts minio.StatObjectOptions) (*minio.ObjectInfo, error) {
	path = pathFix(path, csvContentType)
	return cache.DataExists(path, opts)
}

//
// Readers
//

// GetPROTO reads a PROTO file from minio
func (cache *Cache) GetPROTO(path string, data proto.Message, unmarshalOpts *proto.UnmarshalOptions, opts minio.GetObjectOptions) error {
	path = pathFix(path, jsonContentType)
	cache.logger.Info(fmt.Sprintf("Reading PROTO file, path=%v", path))
	payload, err := cache.ReadData(path, opts)
	if nil != err {
		err = errors.Wrap(err, "Failed to fetch Proto file")
		cache.logger.Error(err.Error())
		return err
	}

	// Deserialize to Proto
	if nil != unmarshalOpts {
		err = unmarshalOpts.Unmarshal(payload, data)
	} else {
		err = proto.Unmarshal(payload, data)
	}
	if nil != err {
		err = errors.Wrap(err, "Failed deserialize data to protobuf")
		cache.logger.Error(err.Error())
		return err
	}

	cache.logger.Info(fmt.Sprintf("Success reading path=%v", path))
	return nil
}

// GetJSON reads a JSON file from minio
func (cache *Cache) GetJSON(path string, output interface{}, opts minio.GetObjectOptions) error {
	path = pathFix(path, jsonContentType)
	cache.logger.Info(fmt.Sprintf("Reading Json file, path=%v", path))
	data, err := cache.ReadData(path, opts)
	if nil != err {
		err = errors.Wrap(err, "Failed to fetch JSON file")
		cache.logger.Error(err.Error())
		return err
	}

	// Deserialize to JSON
	err = json.Unmarshal(data, &output)
	if nil != err {
		err = errors.Wrap(err, "Failed deserialize data from json")
		cache.logger.Error(err.Error())
		return err
	}

	cache.logger.Info(fmt.Sprintf("Success reading path=%v", path))
	return nil
}

// GetCSV reads a CSV file from minio
func (cache *Cache) GetCSV(path string, opts minio.GetObjectOptions) ([][]string, error) {
	path = pathFix(path, csvContentType)
	cache.logger.Info(fmt.Sprintf("Reading CSV file, path=%v", path))
	data, err := cache.ReadData(path, opts)
	if nil != err {
		err = errors.Wrap(err, "Failed to fetch CSV")
		cache.logger.Error(err.Error())
		return nil, err
	}

	buf := bytes.NewReader(data)
	reader := csv.NewReader(buf)
	output, err := reader.ReadAll()
	if nil != err {
		err = errors.Wrap(err, "Failed deserialize data from CSV")
		cache.logger.Error(err.Error())
		return nil, err
	}

	cache.logger.Info(fmt.Sprintf("Success reading path=%v", path))
	return output, err
}

//
// Writers
//

// PutPROTO writes a PROTO file to minio
func (cache *Cache) PutPROTO(path string, data proto.Message, marshalOpts *proto.MarshalOptions, opts minio.PutObjectOptions) error {
	var payload []byte
	var err error
	// Serialize to Proto
	if nil != marshalOpts {
		payload, err = marshalOpts.Marshal(data)
	} else {
		payload, err = proto.Marshal(data)
	}

	if nil != err {
		err = errors.Wrap(err, "Failed serialize data to protobuf")
		cache.logger.Error(err.Error())
		return err
	}
	// Write the data
	opts.ContentType = protobufContentType
	path = pathFix(path, opts.ContentType)
	return cache.WriteData(path, payload, opts)
}

// PutJSON writes a JSON file to minio
func (cache *Cache) PutJSON(path string, data interface{}, opts minio.PutObjectOptions) error {
	// Serialize to JSON
	payload, err := json.Marshal(data)
	if nil != err {
		err = errors.Wrap(err, "Failed serialize data as json")
		cache.logger.Error(err.Error())
		return err
	}
	// Write the data
	opts.ContentType = jsonContentType
	path = pathFix(path, opts.ContentType)
	return cache.WriteData(path, payload, opts)
}

// PutCSV writes a CSV file to minio
func (cache *Cache) PutCSV(path string, records [][]string, opts minio.PutObjectOptions) error {
	// Serialize the CSV to bytes
	buf := &bytes.Buffer{}
	writer := csv.NewWriter(buf)
	if err := writer.WriteAll(records); nil != err {
		err = errors.Wrap(err, "Failed serialize data as CSV")
		cache.logger.Error(err.Error())
		return err
	}

	payload, err := ioutil.ReadAll(buf)
	if nil != err {
		err = errors.Wrap(err, "Failed read bytes from buffer")
		cache.logger.Error(err.Error())
		return err
	}
	// Write the data
	opts.ContentType = csvContentType
	path = pathFix(path, opts.ContentType)
	return cache.WriteData(path, payload, opts)
}

//
// Internal Helpers for accessing the cache directly
//

// DataExists checks to see if the given path exists
func (cache *Cache) DataExists(path string, opts minio.StatObjectOptions) (*minio.ObjectInfo, error) {
	data, err := cache.client.StatObject(cache.ctx, cache.bucketName, path, opts)
	if nil != err {
		cache.logger.Info(fmt.Sprintf("Object doesnt exist in cache at path=%v", path))
		return nil, nil
	}
	return &data, nil
}

// ReadData reads the raw bytes from the minio Cache
func (cache *Cache) ReadData(path string, opts minio.GetObjectOptions) ([]byte, error) {
	cache.logger.Info(fmt.Sprintf("Reading path=%v", path))

	obj, err := cache.client.GetObject(cache.ctx, cache.bucketName, path, opts)
	if nil != err {
		err = errors.Wrap(err, "Failed to get file")
		cache.logger.Error(err.Error())
		return nil, err
	}

	data, err := ioutil.ReadAll(obj)
	if nil != err {
		err = errors.Wrap(err, "Failed to read file")
		return nil, err
	}

	cache.logger.Info(fmt.Sprintf("Successfully read bytes: %v", len(data)))
	return data, nil
}

// WriteData writes the raw bytes from the minio Cache
func (cache *Cache) WriteData(path string, data []byte, opts minio.PutObjectOptions) error {
	cache.logger.Info(fmt.Sprintf("Writing path=%v with %v bytes", path, len(data)))

	reader := bytes.NewReader(data)
	uploadInfo, err := cache.client.PutObject(cache.ctx, cache.bucketName, path, reader, reader.Size(), opts)
	if nil != err {
		return err
	}

	cache.logger.Info(fmt.Sprintf("Successfully uploaded bytes: %v", uploadInfo.Size))
	return nil
}

const jsonContentType = "application/json"
const csvContentType = "text/csv"
const protobufContentType = "application/x-protobuf"

var defaultExtensions map[string]string

func init() {
	defaultExtensions = map[string]string{
		jsonContentType:     "json",
		csvContentType:      "csv",
		protobufContentType: "pb",
	}
}

func pathFix(path, contentType string) string {
	ext := filepath.Ext(path)
	expected, ok := defaultExtensions[contentType]
	if !ok || ext == expected {
		return path
	}
	return fmt.Sprintf("%v.%v", path, expected)
}
