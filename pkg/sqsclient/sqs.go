package sqsclient

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/sirupsen/logrus"
)

// SQSClient encapsula o cliente SQS.
type SQSClient struct {
	Client      *sqs.Client
	QueueURL    string
	Region      string
	EndpointURL string
	logger      *logrus.Entry
	context     context.Context
}

// NewSQSClient cria e configura um novo cliente SQS.
func NewSQSClient(ctx context.Context, region, endpointURL, queueName string, awsKey string, awsSecret string) (*SQSClient, error) {
	logger := logrus.WithFields(logrus.Fields{
		"component":  "SQSClient",
		"queue_name": queueName,
		"region":     region,
	})

	var cfg aws.Config
	var err error

	if err != nil {
		logger.WithError(err).Error("Erro ao carregar a configuração do SDK da AWS")
		return nil, fmt.Errorf("erro ao carregar a configuração do SDK: %w", err)
	}

	var sqsClient *sqs.Client

	if endpointURL == "" {
		logrus.Warn("create sqs without endpoint")
		sqsClient = sqs.NewFromConfig(cfg)
	} else {
		sqsClient = sqs.NewFromConfig(cfg, func(options *sqs.Options) {
			options.BaseEndpoint = aws.String(endpointURL)
			options.EndpointResolverV2 = sqsEndpointResolver{}
			if awsKey != "" && awsSecret != "" {
				options.Credentials = credentials.NewStaticCredentialsProvider(awsKey, awsSecret, "")
			}
		})
	}

	urlResult, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		logger.WithError(err).Errorf("Não foi possível obter a URL da fila '%s'", queueName)
		return nil, fmt.Errorf("falha ao obter a URL da fila: %w", err)
	}

	queueURL := *urlResult.QueueUrl

	return &SQSClient{
		Client:      sqsClient,
		QueueURL:    queueURL,
		Region:      region,
		EndpointURL: endpointURL,
		logger:      logger.WithField("queue_url", queueURL),
		context:     context.Background(),
	}, nil
}

// SendMessage envia uma mensagem para a fila.
func (c *SQSClient) SendMessage(messageBody string) (*sqs.SendMessageOutput, error) {
	c.logger.Infof("Enviando mensagem...")
	input := &sqs.SendMessageInput{
		MessageBody: aws.String(messageBody),
		QueueUrl:    aws.String(c.QueueURL),
	}

	result, err := c.Client.SendMessage(c.context, input)
	if err != nil {
		c.logger.WithError(err).Error("Falha ao enviar mensagem")
		return nil, fmt.Errorf("falha ao enviar mensagem: %w", err)
	}

	c.logger.Infof("Mensagem enviada com sucesso! ID: %s", *result.MessageId)
	return result, nil
}

// ReceiveMessages recebe mensagens da fila.
func (c *SQSClient) ReceiveMessages(maxMessages int32, waitTimeSeconds int32) (*sqs.ReceiveMessageOutput, error) {
	c.logger.Info("Aguardando para receber mensagens...")
	input := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.QueueURL),
		MaxNumberOfMessages: maxMessages,
		WaitTimeSeconds:     waitTimeSeconds, // Long polling
	}

	result, err := c.Client.ReceiveMessage(c.context, input)
	if err != nil {
		c.logger.WithError(err).Error("Falha ao receber mensagens")
		return nil, fmt.Errorf("falha ao receber mensagens: %w", err)
	}

	return result, nil
}

// DeleteMessage apaga uma mensagem da fila após o processamento.
func (c *SQSClient) DeleteMessage(message types.Message) error {
	c.logger.Infof("Apagando mensagem ID: %s", *message.MessageId)
	input := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.QueueURL),
		ReceiptHandle: message.ReceiptHandle,
	}

	result, err := c.Client.DeleteMessage(c.context, input)
	if err != nil {
		c.logger.WithError(err).Errorf("Falha ao apagar mensagem ID: %s", *message.MessageId)
		return fmt.Errorf("falha ao apagar mensagem: %w", err)
	}

	c.logger.Infof("Result %v", result.ResultMetadata)

	c.logger.Infof("Mensagem ID: %s apagada com sucesso", *message.MessageId)
	return nil
}
