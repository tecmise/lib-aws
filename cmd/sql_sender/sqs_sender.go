package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tecmise/lib-aws/pkg/sqsclient"
)

func main() {
	// --- Configurações ---
	region := "us-east-1"
	endpointURL := "http://localhost:4566" // Endpoint do LocalStack
	queueName := "student_sheet"           // Nome da fila no SQS

	ctx := context.Background()

	// 1. Cria o cliente SQS
	client, err := sqsclient.NewSQSClient(ctx, region, endpointURL, queueName)
	if err != nil {
		log.Fatalf("Falha ao criar o cliente SQS: %v", err)
	}
	fmt.Printf("Cliente SQS conectado à fila '%s'\n", queueName)

	// 2. Envia uma mensagem
	mensagem := fmt.Sprintf("Olá, SQS! A hora é %s", time.Now().Format(time.RFC3339))
	_, err = client.SendMessage(mensagem)
	if err != nil {
		log.Fatalf("Falha ao enviar a mensagem: %v", err)
	}

	fmt.Println("Programa emissor finalizado com sucesso.")
}
