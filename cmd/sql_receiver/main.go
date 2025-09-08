package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tecmise/lib-aws/pkg/sqsclient"
)

func main() {
	region := "us-east-1"
	endpointURL := "http://localhost:4566"
	queueName := "student-sheet-queue"

	ctx := context.Background()

	// 1. Cria o cliente SQS
	client, err := sqsclient.NewSQSClient(ctx, region, endpointURL, queueName, "test", "test")
	if err != nil {
		log.Fatalf("Falha ao criar o cliente SQS: %v", err)
	}
	fmt.Printf("Cliente SQS conectado e escutando a fila '%s'. Pressione Ctrl+C para sair.\n", queueName)

	// 2. Loop infinito para receber mensagens (Long Polling)
	for {
		output, err := client.ReceiveMessages(10, 20) // Recebe até 10 msgs, espera até 20 seg
		if err != nil {
			log.Printf("Erro ao receber mensagens: %v. Tentando novamente em 5 segundos...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if len(output.Messages) == 0 {
			fmt.Println("Nenhuma mensagem recebida, aguardando...")
			continue
		}

		fmt.Printf("Recebidas %d mensagens!\n", len(output.Messages))

		// 3. Processa e apaga cada mensagem
		for _, msg := range output.Messages {
			fmt.Printf("  - Processando Mensagem ID: %s\n", *msg.MessageId)
			fmt.Printf("    Conteúdo: %s\n", *msg.Body)

			// Apaga a mensagem da fila para não ser processada novamente
			err := client.DeleteMessage(msg)
			if err != nil {
				log.Printf("    ERRO ao apagar mensagem ID %s: %v", *msg.MessageId, err)
			}
		}
	}
}
