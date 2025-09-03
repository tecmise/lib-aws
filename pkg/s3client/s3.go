package s3client

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sirupsen/logrus"
)

// S3Client encapsula o cliente S3 e as informações de configuração.
type S3Client struct {
	Client      *s3.Client
	BucketName  string
	Region      string
	EndpointURL string
	logger      *logrus.Entry
	context     context.Context
}

// NewS3Client é o construtor que cria e configura o cliente S3.
func NewS3Client(ctx context.Context, bucket, region, endpointURL string, awsKey string, awsSecret string) (*S3Client, error) {
	logger := logrus.WithFields(logrus.Fields{
		"component": "S3Client",
		"bucket":    bucket,
		"region":    region,
	})

	// 1. Criamos um resolvedor de endpoint customizado usando a função nativa do SDK.
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, r string, options ...interface{}) (aws.Endpoint, error) {
		if service == s3.ServiceID && r == region {
			return aws.Endpoint{
				URL:               endpointURL,
				SigningRegion:     region,
				Source:            aws.EndpointSourceCustom,
				HostnameImmutable: true,
			}, nil
		}
		// fallback para o resolvedor padrão
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	// 2. Forçamos o uso de credenciais estáticas para garantir que a requisição seja assinada.
	creds := credentials.NewStaticCredentialsProvider(awsKey, awsSecret, "")

	// 3. Carregamos a configuração com todas as nossas opções.
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(creds),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		logger.WithError(err).Error("Erro ao carregar a configuração do SDK da AWS")
		return nil, fmt.Errorf("erro ao carregar a configuração do SDK: %w", err)
	}

	// 4. Criamos o cliente S3, garantindo o uso de "path-style" que o LocalStack requer.
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	return &S3Client{
		Client:      s3Client,
		BucketName:  bucket,
		Region:      region,
		EndpointURL: endpointURL,
		logger:      logger,
		context:     context.Background(),
	}, nil
}

// ListObjects lista os objetos na raiz do bucket.
func (c *S3Client) ListObjects() (*s3.ListObjectsV2Output, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.BucketName),
	}
	result, err := c.Client.ListObjectsV2(c.context, input)
	if err != nil {
		c.logger.WithError(err).Error("Erro ao listar objetos no bucket")
		return nil, fmt.Errorf("erro ao listar objetos no bucket %s: %w", c.BucketName, err)
	}
	return result, nil
}

// UploadObject envia dados para o bucket S3.
func (c *S3Client) UploadObject(objectKey string, data []byte) (*s3.PutObjectOutput, error) {
	c.logger.Infof("Iniciando upload para o bucket '%s', chave '%s'", c.BucketName, objectKey)
	input := &s3.PutObjectInput{
		Bucket: aws.String(c.BucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(data),
	}
	result, err := c.Client.PutObject(c.context, input)
	if err != nil {
		c.logger.WithError(err).Errorf("Falha no upload do objeto para a chave '%s'", objectKey)
		return nil, fmt.Errorf("falha ao fazer upload para %s/%s: %w", c.BucketName, objectKey, err)
	}
	c.logger.Infof("Upload para a chave '%s' concluído com sucesso", objectKey)
	return result, nil
}
