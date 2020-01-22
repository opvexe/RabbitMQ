package main

import (
	"fmt"
	rabbitmq2 "shumin-project/Rabbitmq/rabbitmq"
)

func main() {
	rabbitmq ,err := rabbitmq2.NewRabbitMQSimple("musicSimple")
	fmt.Println(err)
	rabbitmq.ConsumeSimple()
}
