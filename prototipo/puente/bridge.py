import json
import os
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from jsonschema import Draft202012Validator
from kafka import KafkaProducer

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda:9092")
TOPIC_TELEMETRIA = "ot.telemetry.v1"
SCHEMA_PATH = os.getenv("SCHEMA_PATH", "/app/schemas/telemetria-lectura.schema.json")

with open(SCHEMA_PATH) as f:
    SCHEMA = json.load(f)


VALIDADOR = Draft202012Validator(SCHEMA)

productor = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS.split(","),
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5,
)


def normalizar_a_cloudevent(lectura_cruda: dict) -> dict:
    tramo_id = lectura_cruda["tramoId"]
    return {
        "specversion": "1.0",
        "id": str(uuid.uuid4()),
        "source": "urn:andestransporte:puente-ot-it",
        "type": "com.andestransporte.telemetria.lectura.v1",
        "time": datetime.now(timezone.utc).isoformat(),
        "subject": tramo_id,
        "datacontenttype": "application/json",
        "data": {
            "tramoId": tramo_id,
            "sensorId": lectura_cruda["sensorId"],
            "tipoSensor": lectura_cruda["tipoSensor"],
            "rol": lectura_cruda.get("rol", "no_aplica"),
            "valor": lectura_cruda["valor"],
            "unidad": lectura_cruda["unidad"],
            "timestampLectura": lectura_cruda["timestamp"],
            "calidadSenal": lectura_cruda.get("calidad", "incierta"),
        },
    }


def al_recibir_mensaje(client, userdata, msg):
    try:
        lectura_cruda = json.loads(msg.payload.decode("utf-8"))
        evento = normalizar_a_cloudevent(lectura_cruda)

        VALIDADOR.validate(evento)
    except Exception as exc:
        print(f"[puente] evento RECHAZADO, no cruza la DMZ: {exc}")
        return

    try:
        productor.send(TOPIC_TELEMETRIA, key=evento["subject"], value=evento).get(timeout=10)
        print(f"[puente] {evento['data']['sensorId']} -> {TOPIC_TELEMETRIA} (tramo={evento['subject']})")
    except Exception as exc:
        print(f"[puente] *** EVENTO PERDIDO *** {evento['id']} ({evento['data']['sensorId']}) no se pudo publicar en Kafka tras los reintentos: {exc}")


def main() -> None:
    cliente = mqtt.Client(client_id="puente-ot-it")
    cliente.on_message = al_recibir_mensaje
    cliente.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    cliente.subscribe("ot/tramo/+/sensor/+", qos=1)
    print(f"[puente] escuchando ot/tramo/+/sensor/+ en {MQTT_HOST}:{MQTT_PORT}")
    print(f"[puente] publicando en {KAFKA_BROKERS}, topic {TOPIC_TELEMETRIA}")
    cliente.loop_forever()


if __name__ == "__main__":
    main()
