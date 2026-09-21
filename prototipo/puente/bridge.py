import json
import os
import time
import urllib.request
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from jsonschema import Draft202012Validator
from kafka import KafkaProducer

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda:9092")
TOPIC_TELEMETRIA = "ot.telemetry.v1"
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://redpanda:8081")
SUBJECT = f"{TOPIC_TELEMETRIA}-value"


def obtener_esquema_vigente() -> tuple[int, dict]:
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions/latest"
    while True:
        try:
            with urllib.request.urlopen(url, timeout=5) as r:
                resp = json.loads(r.read())
            return resp["version"], json.loads(resp["schema"])
        except Exception as exc:
            print(f"[puente] sin esquema vigente para {SUBJECT} en {SCHEMA_REGISTRY_URL} ({exc}); no se publica nada, reintento en 5 s")
            time.sleep(5)


VERSION_ESQUEMA, SCHEMA = obtener_esquema_vigente()
print(f"[puente] validando contra {SUBJECT} version {VERSION_ESQUEMA} del Schema Registry")
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


def al_conectar(client, userdata, flags, rc):
    if rc != 0:
        print(f"[puente] conexion MQTT rechazada (rc={rc}), se reintentara")
        return
    client.subscribe("ot/tramo/+/sensor/+", qos=1)
    print(f"[puente] conectado a {MQTT_HOST}:{MQTT_PORT}, suscrito a ot/tramo/+/sensor/+")


def al_desconectar(client, userdata, rc):
    if rc != 0:
        print(f"[puente] conexion MQTT perdida (rc={rc}), reconectando...")


def main() -> None:
    cliente = mqtt.Client(client_id="puente-ot-it", clean_session=False)
    cliente.on_connect = al_conectar
    cliente.on_disconnect = al_desconectar
    cliente.on_message = al_recibir_mensaje
    cliente.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    print(f"[puente] publicando en {KAFKA_BROKERS}, topic {TOPIC_TELEMETRIA}")
    cliente.loop_forever()


if __name__ == "__main__":
    main()
