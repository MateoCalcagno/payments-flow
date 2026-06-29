import os
import json
import threading
from fastapi import FastAPI
from pydantic import BaseModel
from pymongo import MongoClient
from bson import ObjectId
from kafka import KafkaProducer
from confluent_kafka import Consumer

app = FastAPI()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:root@localhost:27017/")

client = MongoClient(MONGO_URI)
db = client["payment_flow"]
coleccion = db["ordenes"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class OrdenRequest(BaseModel):
    usuario_id: int
    monto: float
    descripcion: str

class StatusUpdate(BaseModel):
    status: str

def consumir_pagos():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'order-service-group-v2',
        'auto.offset.reset': 'latest',
    })
    consumer.subscribe(['pagos-procesados'])
    print("order-service escuchando pagos-procesados...")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error consumer: {msg.error()}")
            continue
        pago = json.loads(msg.value().decode('utf-8'))
        orden_id = pago['orden_id']
        coleccion.update_one(
            {"_id": ObjectId(orden_id)},
            {"$set": {"status": pago['status']}}
        )
        print(f"📝 Orden {orden_id} actualizada a '{pago['status']}'")

@app.on_event("startup")
def startup():
    thread = threading.Thread(target=consumir_pagos, daemon=True)
    thread.start()

@app.get("/")
def health():
    return {"status": "order-service corriendo"}

@app.post("/ordenes")
def crear_orden(orden: OrdenRequest):
    nueva_orden = {
        "usuario_id": orden.usuario_id,
        "monto": orden.monto,
        "descripcion": orden.descripcion,
        "status": "pendiente"
    }
    resultado = coleccion.insert_one(nueva_orden)
    orden_id = str(resultado.inserted_id)

    producer.send('ordenes-nuevas', {
        "orden_id": orden_id,
        "usuario_id": orden.usuario_id,
        "monto": orden.monto
    })

    return {
        "orden_id": orden_id,
        "usuario_id": orden.usuario_id,
        "monto": orden.monto,
        "status": "pendiente"
    }

@app.get("/ordenes/{orden_id}")
def obtener_orden(orden_id: str):
    orden = coleccion.find_one({"_id": ObjectId(orden_id)})
    if not orden:
        return {"error": "orden no encontrada"}
    return {
        "orden_id": str(orden["_id"]),
        "usuario_id": orden["usuario_id"],
        "monto": orden["monto"],
        "descripcion": orden["descripcion"],
        "status": orden["status"]
    }

@app.put("/ordenes/{orden_id}/status")
def actualizar_status(orden_id: str, body: StatusUpdate):
    coleccion.update_one(
        {"_id": ObjectId(orden_id)},
        {"$set": {"status": body.status}}
    )
    return {"orden_id": orden_id, "status": body.status}