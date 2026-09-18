import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
INTERVALO_SEG = float(os.getenv("INTERVALO_SEG", "5"))
INYECTAR_A_LOS_SEG = int(os.getenv("INYECTAR_ANOMALIA_A_LOS_SEG", "90"))
TRAMOS = [t.strip() for t in os.getenv("TRAMOS", "tramo-14").split(",") if t.strip()]
CONTROL_FILE = os.getenv("CONTROL_FILE", "").strip()

BASE_PRESION_BAR = 45.0
BASE_CAUDAL_M3H = 120.0


def ahora_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def construir_lectura(sensor_id: str, tipo: str, valor: float, unidad: str, rol: str, tramo_id: str) -> dict:
    return {
        "sensorId": sensor_id,
        "tipoSensor": tipo,
        "rol": rol,
        "tramoId": tramo_id,
        "valor": round(valor, 2),
        "unidad": unidad,
        "timestamp": ahora_iso(),
        "calidad": "buena",
    }


def leer_control() -> Optional[dict]:
    if not CONTROL_FILE or not os.path.exists(CONTROL_FILE):
        return None
    try:
        with open(CONTROL_FILE, "r", encoding="utf-8") as f:
            datos = json.load(f)
        if datos.get("modo") == "manual":
            return datos
        return None
    except Exception as exc:
        print(f"[generador] no se pudo leer el archivo de control ({exc}), se ignora y se sigue en modo automatico")
        return None


def main() -> None:
    cliente = mqtt.Client(client_id=f"generador-telemetria-{uuid.uuid4().hex[:6]}")
    cliente.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    cliente.loop_start()

    inicio = time.time()
    anomalia_automatica_activa = False

    print(f"[generador] publicando en {MQTT_HOST}:{MQTT_PORT} para tramos {TRAMOS}")
    print(f"[generador] modo automatico: la anomalia se inyectaria a los {INYECTAR_A_LOS_SEG}s en todos los tramos")
    if CONTROL_FILE:
        print(f"[generador] control manual habilitado via {CONTROL_FILE} (Panel de Operacion) — tiene prioridad sobre el modo automatico")

    while True:
        transcurrido = time.time() - inicio
        if not anomalia_automatica_activa and transcurrido >= INYECTAR_A_LOS_SEG:
            anomalia_automatica_activa = True
            print("[generador] *** INYECTANDO ANOMALIA AUTOMATICA: caida de presion + descuadre de balance (todos los tramos) ***")

        control = leer_control()

        for tramo_id in TRAMOS:
            if control is not None:
                tramo_con_anomalia = tramo_id in control.get("tramosConAnomalia", [])
            else:
                tramo_con_anomalia = anomalia_automatica_activa

            ruido = random.uniform(-0.5, 0.5)

            if tramo_con_anomalia:
                presion = BASE_PRESION_BAR * 0.78 + ruido
                caudal_arriba = BASE_CAUDAL_M3H + ruido
                caudal_abajo = BASE_CAUDAL_M3H * 0.85 + ruido
            else:
                presion = BASE_PRESION_BAR + ruido
                caudal_arriba = BASE_CAUDAL_M3H + ruido
                caudal_abajo = BASE_CAUDAL_M3H + ruido * 1.2

            lecturas = [
                construir_lectura(f"{tramo_id}-pres-01", "presion", presion, "bar", "no_aplica", tramo_id),
                construir_lectura(f"{tramo_id}-caudal-arriba", "caudal", caudal_arriba, "m3h", "aguas_arriba", tramo_id),
                construir_lectura(f"{tramo_id}-caudal-abajo", "caudal", caudal_abajo, "m3h", "aguas_abajo", tramo_id),
                construir_lectura(f"{tramo_id}-temp-01", "temperatura", 24 + ruido, "celsius", "no_aplica", tramo_id),
            ]

            for lectura in lecturas:
                topic = f"ot/tramo/{tramo_id}/sensor/{lectura['sensorId']}"
                cliente.publish(topic, json.dumps(lectura), qos=1)

        time.sleep(INTERVALO_SEG)


if __name__ == "__main__":
    main()
