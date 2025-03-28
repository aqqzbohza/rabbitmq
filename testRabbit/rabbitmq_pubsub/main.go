package main

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// ฟังก์ชันเชื่อมต่อกับ RabbitMQ
func connectRabbitMQ() (*amqp.Connection, *amqp.Channel) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	// ประกาศ Exchange
	err = ch.ExchangeDeclare(
		"logs",   // ชื่อ Exchange
		"fanout", // ประเภท Fanout
		true,     // Durable
		false,    // Auto-delete
		false,    // Internal
		false,    // No-wait
		nil,      // Arguments
	)
	if err != nil {
		log.Fatal(err)
	}

	return conn, ch
}

// ฟังก์ชัน Publisher (ส่งข้อความ)
func publisher() {
	conn, ch := connectRabbitMQ()
	defer conn.Close()
	defer ch.Close()

	for i := 1; i <= 5; i++ {
		body := fmt.Sprintf("Message %d", i)

		// ส่งข้อความไปยัง Exchange
		err := ch.Publish(
			"logs",
			"",
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			},
		)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(" [x] Sent:", body)
		time.Sleep(2 * time.Second) // ส่งข้อความทุก 2 วินาที
	}
}

// ฟังก์ชัน Subscriber (รับข้อความ)
func subscriber(id int) {
	conn, ch := connectRabbitMQ()
	defer conn.Close()
	defer ch.Close()

	// ประกาศ Queue ชั่วคราว
	q, err := ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Bind Queue เข้ากับ Exchange
	err = ch.QueueBind(
		q.Name,
		"",
		"logs",
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	// รับข้อความจาก Queue
	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf(" [*] Subscriber %d waiting for messages...\n", id)

	for msg := range msgs {
		fmt.Printf(" [Subscriber %d] Received: %s\n", id, msg.Body)
	}
}

func main() {
	// เรียกใช้ Publisher และ Subscriber พร้อมกัน
	go publisher()
	go subscriber(1)
	go subscriber(2)
	go subscriber(3)

	// ทำให้โปรแกรมรันต่อไป
	select {}
}
