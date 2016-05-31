package main

import (
	"github.com/aws-sdk-go/aws"
	"github.com/aws-sdk-go/service/dynamodb"
)

func main() {
	svc := dynamodb.New(session.New(&aws.Config{Endpoint: aws.String("http://localhost:8000")}))
	result, err := svc.ListTables(&dynamodb.ListTablesInput{})
	if err != nil {
		log.Println(err)
		return
	}

	log.Println("Tables:")
	for _, table := range result.TableNames {
		log.Println(*table)
	}

}
