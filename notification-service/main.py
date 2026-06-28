import os
import json
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

consumer = Consumer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'notification-group-v3',
    'auto.offset.reset': 'earliest',
})

consumer.subscribe(['pagos-procesados'])

print("notification-service corriendo...")
print("Escuchando pagos procesados...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Error: {msg.error()}")
        continue
    pago = json.loads(msg.value().decode('utf-8'))
    print(f"📧 Notificando usuario {pago['usuario_id']} - Orden {pago['orden_id']} procesada por ${pago['monto']}")