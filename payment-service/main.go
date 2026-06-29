package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
)

type Orden struct {
	OrdenID   string  `json:"orden_id"`
	UsuarioID int     `json:"usuario_id"`
	Monto     float64 `json:"monto"`
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func main() {
	fmt.Println("payment-service corriendo...")

	ctx := context.Background()

	// Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: getEnv("REDIS_ADDR", "localhost:6379"),
	})
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal("Error conectando a Redis:", err)
	}
	fmt.Println("✅ Conectado a Redis")

	// Kafka
	brokers := []string{getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")}

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatal("Error conectando a Kafka:", err)
	}
	defer consumer.Close()

	producer, err := sarama.NewSyncProducer(brokers, nil)
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

		// Idempotencia
		key := fmt.Sprintf("orden_procesada:%s", orden.OrdenID)
		existe, err := rdb.Exists(ctx, key).Result()
		if err != nil {
			log.Printf("Error consultando Redis: %v", err)
			continue
		}
		if existe > 0 {
			fmt.Printf("⚠️  Orden %s ya procesada, ignorando duplicado\n", orden.OrdenID)
			continue
		}

		fmt.Printf("✅ Procesando pago - Orden: %s | Usuario: %d | Monto: $%.2f\n",
			orden.OrdenID, orden.UsuarioID, orden.Monto)

		// Guardar en Redis
		rdb.Set(ctx, key, "procesado", 24*time.Hour)

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