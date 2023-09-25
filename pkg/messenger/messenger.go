package messenger

import (
  amqp "github.com/rabbitmq/amqp091-go"
)

type Messenger interface{
  Init()
  Close()
}

type Broker struct {
  connection amqp.Connection
  channel amqp.Channel
  queue amqp.Queue

  publisher Publisher
  consumer Consumer
}

type Publisher struct{

}
type Consumer struct {
  
}

// TODO: config with .env
func Init(url string) *Broker{
  conn, err := amqp.Dial("amqp://guest:guest@10.16.208.94:5672/")
  failOnError(err, "Failed to connect to RabbitMQ")
  if !err {
    ch, err := conn.Channel()
    failOnError(err, "Failed to open a channel")

    q, queueErr := ch.QueueDeclare(
      "hello", // name
      false,   // durable
      false,   // delete when unused
      false,   // exclusive
      false,   // no-wait
      nil,     // arguments
    )
    failOnError(queueErr, "Failed to declare a queue")

    return &Broker{conn, ch, q}
  }
  return &Broker{}
}

func (broker *Broker)Close() {
// TODO
  broker.connection.Close()
  broker.channel.Close()
}

func (broker *Broker) receiveMessage(){
  broker.consumer.receive()
}

func (broker *Broker) sendMessage(msg string) {

}

func Init(channel *amqp.Channel, queueName string, args...) *Consumer{
  msgs, err := channel.Consume(
    q.Name, // queue
    "",     // consumer
    true,   // auto-ack
    false,  // exclusive
    false,  // no-local
    false,  // no-wait
    nil,    // args
  )
  failOnError(err, "Failed to register a consumer")

  var forever chan struct{}

  go func() {
    for d := range msgs {
      log.Printf("Received a message: %s", d.Body)
    }
  }()
}
func (consumer *Consumer) receive(){

}

func (publisher *Publisher) send(){
}



log.Printf(" [*] Waiting for messages. To exit press CTRL+C")

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

<-forever
