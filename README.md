# Payment Flow 🚀

Sistema de procesamiento de pagos basado en microservicios y mensajería asíncrona.

## Arquitectura

order-service (Python) → Kafka → payment-service (Go) → Kafka → notification-service (Python)

## Servicios

- **order-service** — Crea órdenes y las persiste en MongoDB
- **payment-service** — Procesa pagos consumiendo eventos de Kafka
- **notification-service** — Notifica al usuario cuando el pago fue procesado

## Tecnologías

- Python + FastAPI
- Go + Gin
- Apache Kafka
- MongoDB
- Docker + Docker Compose

## Cómo correrlo

```bash
# Levantar infraestructura
docker compose up -d

# order-service
cd order-service && source venv/bin/activate && uvicorn main:app --reload

# payment-service
cd payment-service && go run main.go

# notification-service
cd notification-service && source venv/bin/activate && python3 main.py
```

## Flujo

1. Usuario crea una orden via REST en order-service
2. order-service guarda la orden en MongoDB y publica en Kafka
3. payment-service consume el evento y procesa el pago
4. payment-service publica el resultado en Kafka
5. notification-service consume el evento y notifica al usuario

## Autor
Mateo Calcagno — Analista en Computación
