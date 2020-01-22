package main

import (
	"fmt"
	rabbitmq2 "shumin-project/Rabbitmq/rabbitmq"
	"strconv"
	"time"
)

func main() {
	rabbitmq ,_:= rabbitmq2.NewRabbitMQSimple("musicSimple")

	for i := 0; i < 1000; i++ {
		rabbitmq.ProduceSimple("hello muisc!"+strconv.Itoa(i))
		time.Sleep(1*time.Second)
		fmt.Println(i)
	}
}
