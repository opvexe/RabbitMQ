package rabbitmq

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"sass-pc-server/utils"
)

type RabbitMQ struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	QueueName string //队列名称
	Exchange  string //交换机
	key       string //key
	MQURL     string //连接信息
}

//初始化
func NewRabbitMQ(queueName, exchange, key string) (*RabbitMQ, error) {
	rabbitmq := &RabbitMQ{
		QueueName: queueName,
		Exchange:  exchange,
		key:       key,
		MQURL:     "amqp://music:muisc@127.0.0.1:5672/music_vhost", //amqp://账号:密码服务器地址:端口号/vhost
	}
	var err error
	rabbitmq.conn, err = amqp.Dial(rabbitmq.MQURL)
	if err != nil {
		return nil, errors.Wrap(err, "create connect error")
	}
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "create connect error")
	}
	return rabbitmq, nil
}

// 简单模式&&工作模式
func NewRabbitMQSimple(queueName string) (*RabbitMQ, error) {
	rabbitMQ, err := NewRabbitMQ(queueName, "", "") //在simple模式下 exchange and key 都为空
	if err != nil {
		return nil, err
	}
	return rabbitMQ, nil
}

//简单模式下生产者
func (r *RabbitMQ) ProduceSimple(message string) error {
	// 2.1 申请队列、 如果队列不存在则会自动创建、 如果存在则跳过创建
	// 保证队列存在、 消息能发送到队列中
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		//是否持久化
		true,
		// 是否自动删除
		false,
		// 是否具有排他性
		false,
		//是否阻塞
		false,
		// 额外属性
		nil,
	)
	if err != nil {
		return errors.Wrap(err, "QueueDeclare err")
	}
	// 2.2 发送消息到队列中
	err = r.channel.Publish(
		r.Exchange,
		r.QueueName,
		// 如果为true  根据exchange 类型 和 routkey规则、 如果无法找到符合条件的队列、那么会把发送完的消息返回给发送者
		false,
		// 如果为true  当exchange发送消息 到队列后发现队列上没有绑定消费者， 则会把消息还给 发送者
		false,
		amqp.Publishing{
			// 消息持久化
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(message),
		},
	)
	if err != nil {
		return errors.Wrap(err, "Publish err")
	}
	return nil
}

//简单模式下消费者
func (r *RabbitMQ) ConsumeSimple() {
	// 3.1 申请队列、 如果队列不存在则会自动创建、 如果存在则跳过创建
	// 保证队列存在、 消息能发送到队列中
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		//是否持久化
		true,
		// 是否自动删除
		false,
		// 是否具有排他性
		false,
		//是否阻塞
		false,
		// 额外属性
		nil,
	)
	if err != nil {
		fmt.Println("QueueDeclare err: ", err)
	}
	message, err := r.channel.Consume(
		r.QueueName,
		// 用来区分多个消费者
		"",
		// 是否自动应答 (消费完了后 是否自动告诉rabbitmq服务 消费完了 默认为true)
		true,
		// 是否具有排他性 (true 就是 创建了自己可见的队列)
		false,
		// 如果为true 表示 不能将同一个 connection 中发送的消息传递给这个connection中的消费者 (默认为false)
		false,
		// 是否阻塞 false为不阻塞  (阻塞就是 这个消费完 下个再进来、我们系统中是需要的)
		true,
		// 其他参数
		nil,
	)
	if err != nil {
		fmt.Println("Consume err: ", err)
	}

	forever := make(chan bool)
	// 启用 goroutine 处理消息
	go func() {
		for d := range message {
			fmt.Printf("Received a message: %s \n", d.Body)
		}
	}()

	// 等待接受消息
	//log.Println("[*] Waiting for message, To exit press CTRL+C")
	utils.DBCNormalLogger.Info("Waiting for message...")
	// 在没有消息处理后 进行阻塞
	<-forever
}

// 订阅模式
func NewRabbitMQSubScrip(exchangeName string) (*RabbitMQ, error) {
	rabbitmq, err := NewRabbitMQ("", exchangeName, "")
	if err != nil {
		return nil, err
	}
	rabbitmq.conn, err = amqp.Dial(rabbitmq.MQURL)
	if err != nil {
		return nil, errors.Wrap(err, "create connect error")
	}
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "create connect error")
	}
	return rabbitmq, nil
}

//订阅生产者
func (r *RabbitMQ) ProduceSubScrip(message string) error {
	//尝试创建交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		//交换机类型  fanout广播类型
		"fanout",
		//是否持久化
		true,
		//是否自动删除
		false,
		//true 表示这个exchange不可以被client 用来推送消息，仅仅用来exchange 与exchange 之间绑定
		false,
		//不阻塞
		false,
		// 额外属性
		nil,
	)
	if err != nil {
		return err
	}
	//发送消息
	err = r.channel.Publish(
		r.Exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	return err
}

//订阅消费者
func (r *RabbitMQ) ConsumeSubScrip() {
	//1.试探性创建交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		//交换机类型  fanout广播类型
		"fanout",
		//是否持久化
		true,
		//是否自动删除
		false,
		//true 表示这个exchange不可以被client 用来推送消息，仅仅用来exchange 与exchange 之间绑定
		false,
		//不阻塞
		false,
		// 额外属性
		nil,
	)
	if err != nil {
		panic(err)
	}
	//2.试探性创建队列，这里的队列名称不要写
	q, err := r.channel.QueueDeclare(
		//不要写，随机生成队列名称
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	//绑定队列到exchange
	err = r.channel.QueueBind(
		q.Name,
		//pub/sub模式下，这里的key 要为空 ==不为空不是订阅模式
		"",
		r.Exchange,
		false,
		nil,
	)
	//消费消息
	messages, err := r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	forever := make(chan bool)
	go func() {
		for d := range messages {
			fmt.Printf("Revieve a message:", d.Body)
		}
	}()
	<-forever
}

//路由模式
func NewRabbitMQRouter(exchangeName string, routtingKey string) (*RabbitMQ, error) {
	rabbitmq, err := NewRabbitMQ("", exchangeName, routtingKey)
	if err != nil {
		return nil, err
	}
	rabbitmq.conn, err = amqp.Dial(rabbitmq.MQURL)
	if err != nil {
		return nil, errors.Wrap(err, "create connect error")
	}
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "create connect error")
	}
	return rabbitmq, nil
}

//路由模式下生产者
func (r *RabbitMQ) ProduceRouting(message string) error {
	//1.尝试创建交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		//要改成direct
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	//发送消息
	err = r.channel.Publish(
		r.Exchange,
		//要设置key值
		r.key,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
}

//路由模式下接收消息
func (r *RabbitMQ) RecieveRouting() {
	//1.试探性创建交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"direct", //direct
		true,     //是否持久化
		false,    //是否自动删除
		false,    //true 表示这个exchange不可以被client 用来推送消息，仅仅用来exchange 与exchange 之间绑定
		false,    //不阻塞
		nil,
	)
	if err != nil {
		panic(err)
	}

	//2.试探性创建队列，这里的队列名称不要写
	q, err := r.channel.QueueDeclare(
		"", //不要写，随机生成队列名称
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	//绑定队列到exchange
	err = r.channel.QueueBind(
		q.Name,
		r.key, //路由模式下 需设置key
		r.Exchange,
		false,
		nil,
	)

	//消费消息
	messages, err := r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	forever := make(chan bool)
	go func() {
		for d := range messages {
			fmt.Printf("Revieve a message:", d.Body)
		}
	}()
	<-forever
}
