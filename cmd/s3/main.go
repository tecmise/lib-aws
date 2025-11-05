package main

import (
	"context"
	"fmt"
	"log"

	"github.com/tecmise/lib-aws/pkg/s3client"
)

func main() {
	c := s3client.Config{
		BucketName:  "s4s",
		Region:      "us-east-1",
		EndpointURL: "http://localhost:4566",
		AwsKey:      "test",
		AwsSecret:   "test",
	}

	// Cria um contexto.
	ctx := context.Background()
	// 2. CORREÇÃO: Use o apelido 's3client' que você definiu na importação
	client, err := s3client.NewS3Client(ctx, c)

	if err != nil {
		log.Fatalf("Falha ao criar o cliente S3: %v", err)
	}

	fmt.Println("Cliente S3 criado com sucesso.")

	// 2. Prepara os dados para o upload
	nomeDoArquivo := "dir/teste-upload3.txt"
	conteudoDoArquivo := []byte("Olá, S3! Este é o conteúdo do meu primeiro arquivo.")

	// 3. Faz o upload do objeto
	fmt.Printf("Fazendo upload do arquivo '%s' para o bucket '%s'...\n", nomeDoArquivo, c.BucketName)
	_, err = client.UploadObject(ctx, nomeDoArquivo, conteudoDoArquivo)
	if err != nil {
		log.Fatalf("Falha ao fazer upload do objeto: %v", err)
	}
	fmt.Println("Upload concluído com sucesso!")

	fmt.Printf("Tentando listar objetos no bucket '%s'...\n", c.BucketName)

	// Lista os objetos no bucket.
	output, err := client.ListObjects(ctx)
	if err != nil {
		log.Fatalf("Falha ao listar objetos: %v", err)
	} // Lista os objetos no bucket.
	// Exibe os objetos encontrados.
	if len(output.Contents) == 0 {
		fmt.Println("Nenhum objeto encontrado.")
	} else {
		fmt.Println("Objetos encontrados:")
		for _, object := range output.Contents {
			fmt.Printf("- Chave: %s, Tamanho: %d\n", *object.Key, object.Size)
		}
	}

	output, err = client.ListObjectsByPrefix(ctx, nomeDoArquivo)
	if err != nil {
		log.Fatalf("Falha ao listar objetos: %v", err)
	}

	// Exibe os objetos encontrados.
	if len(output.Contents) == 0 {
		fmt.Println("Nenhum objeto encontrado.")
	} else {
		fmt.Println("Objetos encontrados:")
		for _, object := range output.Contents {
			fmt.Printf("- Chave: %s, Tamanho: %d\n", *object.Key, object.Size)
		}
	}

	file, err := client.GetObject(ctx, nomeDoArquivo)
	if err != nil {
		log.Fatalf("Falha ao listar objetos: %v", err)
	}

	fmt.Println("Objeto encontrado:", string(file))
}
