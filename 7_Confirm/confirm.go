package main

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	// 連接 RabbitMQ 服務器
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// 創建一個通道
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// 開啟Publish Confirms
	if err := ch.Confirm(false); err != nil {
		log.Fatalf("Channel could not be put into confirm mode: %s", err)
	}

	// 聲明一個隊列
	q, err := ch.QueueDeclare(
		"hello", // 隊列名稱
		false,   // 持久化
		false,   // 刪除未使用的隊列
		false,   // 獨佔隊列（連接關閉時自動刪除）
		false,   // 等待消息的消費者退出時刪除隊列
		nil,     // 額外參數
	)
	failOnError(err, "Fail to declare a queue")

	// 發布消息
	body := "Hello World"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = ch.PublishWithContext(ctx,
		"",           // 交換機名稱
		q.Name+"123", // 隊列名稱
		false,        // 強制性，true表示當找不到匹配的隊列時，消息將被丟棄
		false,        // 立即發布，true表示消息會被立即投遞給消費者，而不會等待
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	failOnError(err, "Failed to publish a message")

	select {
	case confirm := <-ch.NotifyPublish(make(chan amqp.Confirmation, 1)):
		if confirm.Ack {
			log.Println("Message confirmed")
		} else {
			log.Println("Failed to confirm Message")
		}
	case <-time.After(5 * time.Second):
		log.Println("Time out waiting for confirmation")
	}

	// 關閉通道和連接
	log.Println("Exiting...")
}
