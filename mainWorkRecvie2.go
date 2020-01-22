package main

import rabbitmq2 "shumin-project/Rabbitmq/rabbitmq"

func main() {
	rabbitmq ,_:= rabbitmq2.NewRabbitMQSimple("musicSimple")
	rabbitmq.ConsumeSimple()
}
