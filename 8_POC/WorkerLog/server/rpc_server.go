package main

import (
	"context"
	"log"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func fib(n int, callback func(int) error) int {
	prev := 1
	prev2 := 1
	res := 1
	for i := 2; i <= n; i++ {
		time.Sleep(time.Millisecond * 500)
		err := callback(res)
		log.Printf("err: %v", err)
		res = prev + prev2
		prev2, prev = prev, res
	}
	return res
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"rpc_queue", // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, "Fail to declare a queue")

	err = ch.Qos(
		2,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Fail to set Qos")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    //args
	)
	failOnError(err, "Fail to register a consumer")

	var forever chan struct{}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for d := range msgs {
			go func(d amqp.Delivery) {
				n, err := strconv.Atoi(string(d.Body))
				failOnError(err, "Fail to convert body to integer")

				log.Printf(" [.] fib(%d)", n)
				callback := func(num int) error {
					err = ch.PublishWithContext(ctx,
						"",        // exchange
						d.ReplyTo, // routing key
						false,     // mandatory
						false,     // immediate
						amqp.Publishing{
							ContentType:   "text/plain",
							CorrelationId: d.CorrelationId,
							Body:          []byte(strconv.Itoa(num)),
						})
					return err
				}
				// d.Ack(false)
				response := fib(n, callback)

				err = ch.PublishWithContext(ctx,
					"",        // exchange
					d.ReplyTo, // routing key
					false,     // mandatory
					false,     // immediate
					amqp.Publishing{
						ContentType:   "text/plain",
						CorrelationId: d.CorrelationId,
						Body:          []byte(strconv.Itoa(response)),
					})
				failOnError(err, "Fail to publish a message")

				// d.Ack(false)
			}(d)
		}
	}()

	log.Printf(" [*] Awaiting RPC request")
	<-forever
}
