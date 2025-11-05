package s3client

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sirupsen/logrus"
)

// S3Client encapsula o cliente S3 e as informações de configuração.
type (
	S3Client struct {
		Client     *s3.Client
		BucketName string
		Region     string
		logger     *logrus.Entry
	}

	UploadResult struct {
		URL       string
		ETag      string
		VersionID string
	}

	Config struct {
		BucketName  string
		Region      string
		EndpointURL string
		AwsKey      string
		AwsSecret   string
	}
)

// NewS3Client é o construtor que cria e configura o cliente S3.
func NewS3Client(ctx context.Context, c Config) (*S3Client, error) {
	logger := logrus.WithFields(logrus.Fields{
		"component": "S3Client",
		"bucket":    c.BucketName,
		"region":    c.Region,
	})

	var options []func(*config.LoadOptions) error
	options = append(options, config.WithRegion(c.Region))

	if c.EndpointURL != "" {
		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, r string, opts ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID && r == c.Region {
				return aws.Endpoint{
					URL:               c.EndpointURL,
					SigningRegion:     c.Region,
					Source:            aws.EndpointSourceCustom,
					HostnameImmutable: true,
				}, nil
			}
			// fallback para o resolvedor padrão
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
		options = append(options, config.WithEndpointResolverWithOptions(customResolver))
	}

	if c.AwsKey != "" && c.AwsSecret != "" {
		creds := credentials.NewStaticCredentialsProvider(c.AwsKey, c.AwsSecret, "")
		options = append(options, config.WithCredentialsProvider(creds))
	}

	cfg, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		logger.WithError(err).Error("Erro ao carregar a configuração do SDK da AWS")
		return nil, fmt.Errorf("erro ao carregar a configuração do SDK: %w", err)
	}

	// 4. Criamos o cliente S3, garantindo o uso de "path-style" que o LocalStack requer.
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	return &S3Client{
		Client:     s3Client,
		BucketName: c.BucketName,
		Region:     c.Region,
		logger:     logger,
	}, nil
}

// ListObjects lista os objetos na raiz do bucket.
func (c *S3Client) ListObjects(ctx context.Context) (*s3.ListObjectsV2Output, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.BucketName),
	}
	result, err := c.Client.ListObjectsV2(ctx, input)
	if err != nil {
		c.logger.WithError(err).Error("Erro ao listar objetos no bucket")
		return nil, fmt.Errorf("erro ao listar objetos no bucket %s: %w", c.BucketName, err)
	}
	return result, nil
}

// ListObjectsByPrefix lista os objetos com um prefixo específico.
func (c *S3Client) ListObjectsByPrefix(ctx context.Context, prefix string) (*s3.ListObjectsV2Output, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.BucketName),
		Prefix: aws.String(prefix),
	}
	result, err := c.Client.ListObjectsV2(ctx, input)
	if err != nil {
		c.logger.WithError(err).Error("Erro ao listar objetos no bucket")
		return nil, fmt.Errorf("erro ao listar objetos no bucket %s: %w", c.BucketName, err)
	}
	return result, nil
}

// GetObject downloads an object from the S3 bucket.
func (c *S3Client) GetObject(ctx context.Context, objectKey string) ([]byte, error) {
	c.logger.Infof("Iniciando download do objeto '%s' do bucket '%s'", objectKey, c.BucketName)

	input := &s3.GetObjectInput{
		Bucket: aws.String(c.BucketName),
		Key:    aws.String(objectKey),
	}

	result, err := c.Client.GetObject(ctx, input)
	if err != nil {
		c.logger.WithError(err).Errorf("Falha ao baixar o objeto com a chave '%s'", objectKey)
		return nil, fmt.Errorf("falha ao baixar o objeto %s/%s: %w", c.BucketName, objectKey, err)
	}
	defer result.Body.Close()

	// Read the entire object into memory
	data, err := io.ReadAll(result.Body)
	if err != nil {
		c.logger.WithError(err).Errorf("Falha ao ler o conteúdo do objeto com a chave '%s'", objectKey)
		return nil, fmt.Errorf("falha ao ler o conteúdo do objeto %s/%s: %w", c.BucketName, objectKey, err)
	}

	c.logger.Infof("Download do objeto '%s' concluído com sucesso. Tamanho: %d bytes", objectKey, len(data))
	return data, nil
}

// UploadObject envia dados para o bucket S3.
func (c *S3Client) UploadObject(ctx context.Context, objectKey string, data []byte) (*UploadResult, error) {
	c.logger.Infof("Iniciando upload para o bucket '%s', chave '%s'", c.BucketName, objectKey)
	input := &s3.PutObjectInput{
		Bucket: aws.String(c.BucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(data),
	}
	result, err := c.Client.PutObject(ctx, input)
	if err != nil {
		c.logger.WithError(err).Errorf("Falha no upload do objeto para a chave '%s'", objectKey)
		return nil, fmt.Errorf("falha ao fazer upload para %s/%s: %w", c.BucketName, objectKey, err)
	}

	objectURL := c.BucketName + "/" + objectKey

	uploadResult := &UploadResult{
		URL: objectURL,
	}

	if result.ETag != nil {
		uploadResult.ETag = *result.ETag
	}

	c.logger.Infof("Upload para a chave '%s' concluído com sucesso. URL: %s", objectKey, objectURL)
	return uploadResult, nil

}
