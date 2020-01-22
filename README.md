#### 1.1 RabbitMQ 安装

参考文献:https://www.jianshu.com/p/b7f996301835

参考文献:https://yq.aliyun.com/articles/326222

```shell
$ brew install rabbitmq
$ cd /usr/local/Cellar/rabbitmq/3.8.0
$ sbin/rabbitmq-server # 启动服务
# 访问: http://127.0.0.1:15672
# 用户:guest 密码:guest
$ cd /usr/local/Cellar/rabbitmq/3.8.0/sbin
# 创建用户 用户名为admin，并设置密码为123456
./rabbitmqctl add_user admin 123456
# 标记身份
./rabbitmqctl set_user_tags admin administrator
# 关闭
./rabbitmqctl stop
# 添加vhost 
./rabbitmqctl add_vhost micro-push
`amqp://admin:admin@127.0.0.1:5672/micro-push", //amqp://账号:密码服务器地址:端口号/vhost`
```

docker使用:

```shell
$ docker start rabbitmq
$ docker run -d -p 15672:15672  -p  5672:5672  -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin --name rabbitmq --hostname=rabbitmqhostone  rabbitmq:latest
```

设置v-host:

![image-20200122193327965](image-20200122193327965.png)

