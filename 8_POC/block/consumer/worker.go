package main

import (
	"bytes"
	"fmt"
	"log"
	"sync/atomic"

	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

const limitWorker = 50

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"task_queue",
		true,
		false,
		false,
		false,
		amqp.Table{"x-max-priority": 9},
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,
		0,
		false,
	)
	failOnError(err, "Fail to set Qos")

	msgs, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}
	var mu *sync.Mutex = new(sync.Mutex)
	var emulateWorkerCount *atomic.Int32 = new(atomic.Int32)

	go func() {
		for d := range msgs {
			mu.Lock()
			if emulateWorkerCount.Load() <= limitWorker {
				log.Printf("get %d, Unlock", emulateWorkerCount.Load())
				mu.TryLock()
				mu.Unlock()
			}
			log.Printf("Received a message: %s", d.Body)
			fmt.Println(d.MessageId)
			dotCount := bytes.Count(d.Body, []byte("."))
			// d.Ack(false)
			emulateWorkerCount.Add(1)
			go fakeWorker(dotCount, emulateWorkerCount, mu)
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func fakeWorker(dotCount int, emulateWorkerCount *atomic.Int32, mu *sync.Mutex) {
	t := time.Duration(dotCount)
	time.Sleep(t * 3 * time.Second)
	emulateWorkerCount.Add(-1)
	log.Printf("Done")
	log.Println(emulateWorkerCount.Load())
	if emulateWorkerCount.Load() <= limitWorker {
		mu.TryLock()
		mu.Unlock()
	}
}
