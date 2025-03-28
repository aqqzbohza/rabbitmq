package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	// กำหนด flag เพื่อเลือก mode
	mode := flag.String("mode", "worker", "Choose mode: producer or worker")
	message := flag.String("msg", "Hello, RabbitMQ!", "Message to send (for producer)")
	flag.Parse()

	// เชื่อมต่อกับ RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// สร้าง Channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	// สร้าง Queue
	q, err := ch.QueueDeclare(
		"task_queue",
		true,  // Durable
		false, // Auto delete
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		log.Fatal(err)
	}

	if *mode == "producer" {
		runProducer(ch, q.Name, *message)
	} else {
		runWorker(ch, q.Name)
	}
}

// Producer: ส่งข้อความไปยัง Queue
func runProducer(ch *amqp.Channel, queueName string, message string) {
	err := ch.Publish(
		"",        // Exchange
		queueName, // Routing key
		false,     // Mandatory
		false,     // Immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // ทำให้ข้อความไม่หายหลังรีสตาร์ท
			ContentType:  "text/plain",
			Body:         []byte(message),
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(" [x] Sent:", message)
	
}

// Worker: รับข้อความจาก Queue และประมวลผล
func runWorker(ch *amqp.Channel, queueName string) {
	msgs, err := ch.Consume(
		queueName,
		"",
		false, // Auto Ack (false เพื่อให้ Worker ยืนยันข้อความเอง)
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	forever := make(chan bool)

	go func() {
		for msg := range msgs {
			fmt.Println(" [x] Received:", string(msg.Body))
			time.Sleep(2 * time.Second) // จำลองการประมวลผล
			fmt.Println(" [✓] Done")

			// แจ้ง RabbitMQ ว่าประมวลผลเสร็จแล้ว
			msg.Ack(false)
		}
	}()

	fmt.Println(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
