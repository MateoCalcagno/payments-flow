from fastapi import FastAPI
from pydantic import BaseModel
from pymongo import MongoClient
from kafka import KafkaProducer
import json

app = FastAPI()

# MongoDB
client = MongoClient("mongodb://root:root@localhost:27017/")
db = client["payment_flow"]
coleccion = db["ordenes"]

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class OrdenRequest(BaseModel):
    usuario_id: int
    monto: float
    descripcion: str

@app.get("/")
def health():
    return {"status": "order-service corriendo"}

@app.post("/ordenes")
def crear_orden(orden: OrdenRequest):
    # Guardar en MongoDB
    nueva_orden = {
        "usuario_id": orden.usuario_id,
        "monto": orden.monto,
        "descripcion": orden.descripcion,
        "status": "pendiente"
    }
    resultado = coleccion.insert_one(nueva_orden)
    orden_id = str(resultado.inserted_id)

    # Publicar en Kafka
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