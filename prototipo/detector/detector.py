import json
import os
import sqlite3
import threading
import time
import uuid
from collections import OrderedDict
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

from regla import Deteccion, EstadoTramo, Nominal, procesar_muestra, severidad_para

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda:9092")
TOPIC_TELEMETRIA = "ot.telemetry.v1"
TOPIC_ALERTAS = "it.alerts.valve-anomaly.v1"

UMBRAL_PRESION_PCT = float(os.getenv("UMBRAL_PRESION_PCT", "10"))
UMBRAL_BALANCE_M3H = float(os.getenv("UMBRAL_BALANCE_M3H", "8"))
LECTURAS_CONSECUTIVAS = int(os.getenv("LECTURAS_CONSECUTIVAS", "3"))
NOMINAL_PATH = os.getenv("TRAMOS_NOMINAL_PATH", "/app/config/tramos-nominal.json")

DB_PATH = os.getenv("DB_PATH", "/app/data/outbox.db")
MAX_IDS_RECORDADOS = 5000

estado_por_tramo: dict[str, EstadoTramo] = {}
ids_vistos_por_tramo: dict[str, OrderedDict] = {}
db_lock = threading.Lock()


def cargar_nominales(path: str) -> tuple[dict[str, Nominal], Nominal]:
    with open(path, encoding="utf-8") as f:
        cfg = json.load(f)
    por_tramo = {
        t: Nominal(v["presionNominalBar"], v["caudalNominalM3h"])
        for t, v in cfg.get("tramos", {}).items()
    }
    d = cfg["_default"]
    return por_tramo, Nominal(d["presionNominalBar"], d["caudalNominalM3h"])


NOMINALES, NOMINAL_DEFAULT = cargar_nominales(NOMINAL_PATH)
_avisados_sin_ficha: set = set()


def nominal_de(tramo_id: str) -> Nominal:
    n = NOMINALES.get(tramo_id)
    if n is None and tramo_id not in _avisados_sin_ficha:
        _avisados_sin_ficha.add(tramo_id)
        print(f"[detector] AVISO: {tramo_id} no tiene ficha en {NOMINAL_PATH}; se usa _default {NOMINAL_DEFAULT}")
    return n or NOMINAL_DEFAULT


def init_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS outbox_eventos (
            id TEXT PRIMARY KEY,
            topic TEXT NOT NULL,
            clave TEXT NOT NULL,
            payload TEXT NOT NULL,
            publicado INTEGER NOT NULL DEFAULT 0,
            creado_en TEXT NOT NULL
        )
        """
    )
    con.commit()
    return con


def registrar_alerta_en_outbox(con: sqlite3.Connection, topic: str, clave: str, evento: dict) -> None:
    with db_lock, con:
        con.execute(
            "INSERT INTO outbox_eventos (id, topic, clave, payload, publicado, creado_en) VALUES (?,?,?,?,0,?)",
            (evento["id"], topic, clave, json.dumps(evento), datetime.now(timezone.utc).isoformat()),
        )


def poller_outbox(con: sqlite3.Connection, productor: KafkaProducer) -> None:
    while True:
        with db_lock:
            filas = con.execute(
                "SELECT id, topic, clave, payload FROM outbox_eventos WHERE publicado = 0 LIMIT 50"
            ).fetchall()
        for (id_evt, topic, clave, payload) in filas:
            try:
                productor.send(topic, key=clave, value=json.loads(payload)).get(timeout=10)
                with db_lock, con:
                    con.execute("UPDATE outbox_eventos SET publicado = 1 WHERE id = ?", (id_evt,))
                print(f"[detector] outbox: alerta {id_evt} publicada en {topic}")
            except Exception as exc:
                print(f"[detector] outbox: fallo publicando {id_evt}, se reintenta en el proximo ciclo: {exc}")
        time.sleep(0.5)


def es_duplicado(tramo_id: str, id_evento: str) -> bool:
    vistos = ids_vistos_por_tramo.setdefault(tramo_id, OrderedDict())
    if id_evento in vistos:
        return True
    vistos[id_evento] = None
    while len(vistos) > MAX_IDS_RECORDADOS:
        vistos.popitem(last=False)
    return False


def construir_alerta(d: Deteccion) -> dict:
    return {
        "specversion": "1.0",
        "id": str(uuid.uuid4()),
        "source": "urn:andestransporte:detector-anomalias",
        "type": "com.andestransporte.alerta.valvula-ilicita.v1",
        "time": datetime.now(timezone.utc).isoformat(),
        "subject": d.tramo_id,
        "datacontenttype": "application/json",
        "data": {
            "tramoId": d.tramo_id,
            "ventanaInicio": d.inicio_iso,
            "ventanaFin": d.fin_iso,
            "caidaPresionPorcentaje": d.caida_pct,
            "diferenciaBalanceM3h": d.descuadre_m3h,
            "umbralPresionPorcentaje": UMBRAL_PRESION_PCT,
            "umbralBalanceM3h": UMBRAL_BALANCE_M3H,
            "severidad": severidad_para(d.caida_pct, UMBRAL_PRESION_PCT),
            "posibleCausa": "valvula_ilicita_sospechada",
            "eventosOrigenIds": d.ids_origen,
        },
    }


def main() -> None:
    con = init_db()
    productor = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS.split(","),
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    threading.Thread(target=poller_outbox, args=(con, productor), daemon=True).start()

    consumidor = KafkaConsumer(
        TOPIC_TELEMETRIA,
        bootstrap_servers=KAFKA_BROKERS.split(","),
        group_id="detector-anomalias",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )

    print(
        f"[detector] escuchando {TOPIC_TELEMETRIA} en {KAFKA_BROKERS} "
        f"(umbral_presion={UMBRAL_PRESION_PCT}%, umbral_balance={UMBRAL_BALANCE_M3H}m3/h, "
        f"persistencia={LECTURAS_CONSECUTIVAS} muestras, nominales={NOMINAL_PATH}: {sorted(NOMINALES)})"
    )

    iterador = iter(consumidor)
    while True:
        try:
            msg = next(iterador)
        except Exception as exc:
            print(f"[detector] error consumiendo mensaje, se descarta y se continua: {exc}")
            continue

        try:
            procesar_evento(con, msg.value)
        except Exception as exc:
            print(f"[detector] error procesando mensaje, se descarta: {exc}")
        finally:
            try:
                consumidor.commit()
            except Exception as exc:
                print(f"[detector] no se pudo confirmar el offset (se reintentara con el siguiente): {exc}")


def procesar_evento(con: sqlite3.Connection, evento: dict) -> None:
    data = evento["data"]
    tramo_id = data["tramoId"]

    if es_duplicado(tramo_id, evento["id"]):
        print(f"[detector] duplicado {evento['id']} ({tramo_id}) ignorado")
        return
    if data.get("calidadSenal") == "mala":
        print(f"[detector] lectura {evento['id']} ({tramo_id}, {data['sensorId']}) con calidad 'mala' descartada")
        return

    estado = estado_por_tramo.setdefault(tramo_id, EstadoTramo())
    tipo, rol = data["tipoSensor"], data.get("rol")

    if tipo == "caudal" and rol == "aguas_arriba":
        estado.caudal_arriba = data["valor"]
        return
    if tipo == "caudal" and rol == "aguas_abajo":
        estado.caudal_abajo = data["valor"]
        return
    if tipo != "presion":
        return

    estado.presion = data["valor"]
    estaba_anomalo = estado.anomalo
    deteccion = procesar_muestra(
        estado, tramo_id, nominal_de(tramo_id),
        tiempo_iso=evento.get("time") or datetime.now(timezone.utc).isoformat(),
        id_evento=evento["id"],
        umbral_presion_pct=UMBRAL_PRESION_PCT,
        umbral_balance_m3h=UMBRAL_BALANCE_M3H,
        lecturas_consecutivas=LECTURAS_CONSECUTIVAS,
    )
    if deteccion:
        alerta = construir_alerta(deteccion)
        registrar_alerta_en_outbox(con, TOPIC_ALERTAS, tramo_id, alerta)
        d = alerta["data"]
        print(
            f"[detector] *** ANOMALIA en {tramo_id}: caida={d['caidaPresionPorcentaje']}% "
            f"balance={d['diferenciaBalanceM3h']}m3/h severidad={d['severidad']} "
            f"(confirmada con {LECTURAS_CONSECUTIVAS} muestras) ***"
        )
    elif estaba_anomalo and not estado.anomalo:
        print(f"[detector] {tramo_id} volvio a rango nominal; fin del episodio (una nueva anomalia generara una alerta nueva)")


if __name__ == "__main__":
    main()
