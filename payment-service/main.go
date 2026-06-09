package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

type Orden struct {
	OrdenID   string  `json:"orden_id"`
	UsuarioID int     `json:"usuario_id"`
	Monto     float64 `json:"monto"`
}

func main() {
	fmt.Println("payment-service corriendo...")

	// Consumer
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatal("Error conectando a Kafka:", err)
	}
	defer consumer.Close()

	// Producer
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatal("Error creando producer:", err)
	}
	defer producer.Close()

	partitionConsumer, err := consumer.ConsumePartition("ordenes-nuevas", 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal("Error consumiendo topic:", err)
	}
	defer partitionConsumer.Close()

	fmt.Println("Escuchando ordenes nuevas...")

	for mensaje := range partitionConsumer.Messages() {
		var orden Orden
		json.Unmarshal(mensaje.Value, &orden)
		fmt.Printf("✅ Procesando pago - Orden: %s | Usuario: %d | Monto: $%.2f\n",
			orden.OrdenID, orden.UsuarioID, orden.Monto)

		// Publicar en pagos-procesados
		pagoBytes, _ := json.Marshal(map[string]interface{}{
			"orden_id":   orden.OrdenID,
			"usuario_id": orden.UsuarioID,
			"monto":      orden.Monto,
			"status":     "procesado",
		})

		msg := &sarama.ProducerMessage{
			Topic: "pagos-procesados",
			Value: sarama.ByteEncoder(pagoBytes),
		}
		producer.SendMessage(msg)
		fmt.Printf("📨 Pago publicado en Kafka\n")
	}
}