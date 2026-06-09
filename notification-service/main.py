from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'pagos-procesados',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("notification-service corriendo...")
print("Escuchando pagos procesados...")

for mensaje in consumer:
    pago = mensaje.value
    print(f"📧 Notificando usuario {pago['usuario_id']} - Orden {pago['orden_id']} procesada por ${pago['monto']}")