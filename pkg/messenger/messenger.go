package messenger

import (
  amqp "github.com/rabbitmq/amqp091-go"
  "context"
  "crypto/sha1"
  "os"
  "log"
  "fmt"
  "time"
)


type Messenger interface{
  Init(url string)
  PublishOn(topic string, messages <- chan Message)
  ReceiveOn(topic string, messages chan<- Message)
  Close()
}

type Broker struct{
  url string
  ctx context.Context
  done context.CancelFunc
}


// exchange binds the publishers to the subscribers
const exchange = "topic"

// Message is the application type for a Message.  This can contain identity,
// or a reference to the receiver chan for further demuxing.
type Message []byte

// session composes an amqp.Connection with an amqp.Channel
type session struct {
	*amqp.Connection
	*amqp.Channel
}

func (broker *Broker)Init(url string){
  broker.url = url
  ctx, done := context.WithCancel(context.Background())
  broker.ctx = ctx
  broker.done = done
}

func (broker *Broker)Close(){
  broker.done()
}

func (broker *Broker)PublishOn(topic string, messages <- chan Message){
  go func(){
    publish(redial(broker.ctx, broker.url),topic, messages)
    broker.done()
  }()
}

func (broker *Broker)ReceiveOn(topic string, messages chan <- Message){
  go func(){
    subscribe(redial(broker.ctx, broker.url),topic, messages)
    broker.done()
  }()
}


// Close tears the connection down, taking the channel with it.
func (s session) Close() error {
	if s.Connection == nil {
		return nil
	}
	return s.Connection.Close()
}

// redial continually connects to the URL, exiting the program when no longer possible
func redial(ctx context.Context, url string) chan chan session {
	sessions := make(chan chan session)

	go func() {
		sess := make(chan session)
		defer close(sessions)

		for {
			select {
			case sessions <- sess:
			case <-ctx.Done():
				log.Println("shutting down session factory")
				return
			}

			conn, err := amqp.Dial(url)
			if err != nil {
				log.Fatalf("cannot (re)dial: %v: %q", err, url)
			}

			ch, err := conn.Channel()
			if err != nil {
				log.Fatalf("cannot create channel: %v", err)
			}

			if err := ch.ExchangeDeclare(exchange, "fanout", false, true, false, false, nil); err != nil {
				log.Fatalf("cannot declare fanout exchange: %v", err)
			}

			select {
			case sess <- session{conn, ch}:
        // log.Println("Created session: ", conn, ch)
			case <-ctx.Done():
				log.Println("shutting down new session")
				return
			}
		}
	}()

	return sessions
}

// publish publishes messages to a reconnecting session to a fanout exchange.
// It receives from the application specific source of messages.
func publish(sessions chan chan session, topic string, messages <-chan Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for session := range sessions {
		var (
			running bool
			reading = messages
			pending = make(chan Message, 1)
			confirm = make(chan amqp.Confirmation, 1)
		)

		pub := <-session

		// publisher confirms for this channel/connection
		if err := pub.Confirm(false); err != nil {
			log.Printf("publisher confirms not supported")
			close(confirm) // confirms not supported, simulate by always nacking
		} else {
			pub.NotifyPublish(confirm)
		}

		log.Printf("publishing...")

	Publish:
		for {
			var body Message
			select {
			case confirmed, ok := <-confirm:
				if !ok {
					break Publish
				}
				if !confirmed.Ack {
					log.Printf("nack message %d, body: %q", confirmed.DeliveryTag, string(body))
				}
				reading = messages

			case body = <-pending:
				// routingKey := "ignored for fanout exchanges, application dependent for other exchanges"
				routingKey := topic
				err := pub.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
					Body: body,
				})
				// Retry failed delivery on the next session
				if err != nil {
					pending <- body
					pub.Close()
					break Publish
				}

			case body, running = <-reading:
				// all messages consumed
        fmt.Println("publish: ", string( body ))
				if !running {
					return
				}
				// work on pending delivery until ack'd
				pending <- body
				reading = nil
			}
		}
	}
}

// identity returns the same host/process unique string for the lifetime of
// this process so that subscriber reconnections reuse the same queue name.
func identity() string {
	hostname, err := os.Hostname()
	h := sha1.New()
	fmt.Fprint(h, hostname)
	fmt.Fprint(h, err)
	fmt.Fprint(h, os.Getpid())
	return fmt.Sprintf("%x", h.Sum(nil))
}

// subscribe consumes deliveries from an exclusive queue from a fanout exchange and sends to the application specific messages chan.
func subscribe(sessions chan chan session, topic string, messages chan<- Message) {
	queue := identity()

	for session := range sessions {
		sub := <-session

		if _, err := sub.QueueDeclare(queue, false, true, true, false, nil); err != nil {
			log.Printf("cannot consume from exclusive queue: %q, %v", queue, err)
			return
		}

		// routingKey := "application specific routing key for fancy topologies"
		routingKey := topic
    log.Println("Receive routingKey: ", routingKey)
		if err := sub.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
			log.Printf("cannot consume without a binding to exchange: %q, %v", exchange, err)
			return
		}

		deliveries, err := sub.Consume(queue, "", false, true, false, false, nil)
		if err != nil {
			log.Printf("cannot consume from: %q, %v", queue, err)
			return
		}

		log.Printf("subscribed...")

		for msg := range deliveries {
			messages <- msg.Body
			sub.Ack(msg.DeliveryTag, false)
		}
	}
}

