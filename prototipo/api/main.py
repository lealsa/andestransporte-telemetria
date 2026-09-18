import json
import os
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Body, FastAPI, HTTPException
from kafka import KafkaConsumer

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda:9092")
NOMINAL_PATH = os.getenv("TRAMOS_NOMINAL_PATH", "")

estado_tramos: dict = {}
lock = threading.Lock()


NOMINALES: dict = {}
NOMINAL_DEFAULT: dict | None = None
if NOMINAL_PATH and os.path.exists(NOMINAL_PATH):
    with open(NOMINAL_PATH, encoding="utf-8") as f:
        _cfg = json.load(f)
    NOMINALES = _cfg.get("tramos", {})
    NOMINAL_DEFAULT = _cfg.get("_default")


def _asegurar_tramo(tramo_id: str) -> None:
    estado_tramos.setdefault(
        tramo_id,
        {
            "ultimaLectura": None,
            "lecturas": {"presion": None, "caudal_arriba": None, "caudal_abajo": None, "temperatura": None},
            "ultimaAlerta": None,
            "historialAlertas": [],
            "balanceVolumetrico": None,
            "alertaReconocidaId": None,
        },
    )


def _clave_sensor(data: dict) -> str | None:
    tipo, rol = data.get("tipoSensor"), data.get("rol")
    if tipo == "presion":
        return "presion"
    if tipo == "temperatura":
        return "temperatura"
    if tipo == "caudal" and rol in ("aguas_arriba", "aguas_abajo"):
        return "caudal_arriba" if rol == "aguas_arriba" else "caudal_abajo"
    return None


def _consumir_resiliente(consumidor: KafkaConsumer, procesar) -> None:
    iterador = iter(consumidor)
    while True:
        try:
            msg = next(iterador)
        except StopIteration:
            return
        except Exception as exc:
            print(f"[api] error consumiendo mensaje, se descarta y se continua: {exc}")
            continue
        try:
            procesar(msg.value)
        except Exception as exc:
            print(f"[api] error procesando mensaje, se descarta: {exc}")


def _nuevo_consumidor(topic: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKERS.split(","),
        group_id=f"api-modelo-lectura-{uuid.uuid4().hex[:8]}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )


def consumir_telemetria() -> None:
    def procesar(evento: dict) -> None:
        data = evento["data"]
        tramo_id = data["tramoId"]
        clave = _clave_sensor(data)
        with lock:
            _asegurar_tramo(tramo_id)
            estado_tramos[tramo_id]["ultimaLectura"] = evento
            if clave:
                estado_tramos[tramo_id]["lecturas"][clave] = evento

    _consumir_resiliente(_nuevo_consumidor("ot.telemetry.v1"), procesar)


def consumir_alertas() -> None:
    def procesar(evento: dict) -> None:
        tramo_id = evento["data"]["tramoId"]
        with lock:
            _asegurar_tramo(tramo_id)
            estado_tramos[tramo_id]["ultimaAlerta"] = evento
            estado_tramos[tramo_id]["historialAlertas"].append(evento)

    _consumir_resiliente(_nuevo_consumidor("it.alerts.valve-anomaly.v1"), procesar)


@asynccontextmanager
async def lifespan(_: FastAPI):
    threading.Thread(target=consumir_telemetria, daemon=True).start()
    threading.Thread(target=consumir_alertas, daemon=True).start()
    yield


app = FastAPI(
    title="API de Estado del Tramo — AndesTransporte",
    description=(
        "Expone el estado consolidado (telemetria + alertas) de cada tramo de "
        "ducto, construido a partir de los topics ot.telemetry.v1 e "
        "it.alerts.valve-anomaly.v1. Incluye tambien el stub del Sistema de "
        "Balance Volumetrico usado por el flujo de integracion Camel."
    ),
    version="1.1.0",
    lifespan=lifespan,
)


@app.get("/health", summary="Estado del servicio")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/tramos", summary="Listar tramos con datos conocidos")
def listar_tramos():
    with lock:
        return {"tramos": list(estado_tramos.keys())}


def _nivel(estado: dict) -> str:
    ultima = estado["ultimaAlerta"]
    if not ultima:
        return "normal"
    if estado.get("alertaReconocidaId") == ultima.get("id"):
        return "normal"
    return "alerta_activa"


def _valor(ev: dict | None):
    return ev["data"]["valor"] if ev else None


def _metricas(tramo_id: str, estado: dict) -> dict:
    lec = estado["lecturas"]
    presion, arriba, abajo = _valor(lec["presion"]), _valor(lec["caudal_arriba"]), _valor(lec["caudal_abajo"])
    nominal = NOMINALES.get(tramo_id) or NOMINAL_DEFAULT
    m = {
        "presionBar": presion,
        "caudalArribaM3h": arriba,
        "caudalAbajoM3h": abajo,
        "temperaturaC": _valor(lec["temperatura"]),
        "descuadreM3h": round(arriba - abajo, 2) if None not in (arriba, abajo) else None,
        "nominal": nominal,
        "caidaPresionPct": None,
    }
    if nominal and presion is not None and nominal.get("presionNominalBar"):
        m["caidaPresionPct"] = round(100 * (nominal["presionNominalBar"] - presion) / nominal["presionNominalBar"], 2)
    return m


@app.get("/tramos/{tramo_id}/estado", summary="Estado consolidado de un tramo")
def estado_tramo(tramo_id: str):
    with lock:
        estado = estado_tramos.get(tramo_id)
        if not estado:
            raise HTTPException(status_code=404, detail=f"No hay datos para el tramo '{tramo_id}' todavia")
        return {"tramoId": tramo_id, "nivel": _nivel(estado), "metricas": _metricas(tramo_id, estado), **estado}


@app.post("/tramos/{tramo_id}/alertas/reconocer", summary="Reconocer (acusar recibo de) la alerta activa del tramo")
def reconocer_alerta(tramo_id: str):
    with lock:
        estado = estado_tramos.get(tramo_id)
        if not estado:
            raise HTTPException(status_code=404, detail=f"No hay datos para el tramo '{tramo_id}' todavia")
        ultima = estado["ultimaAlerta"]
        if not ultima:
            return {"tramoId": tramo_id, "nivel": "normal", "reconocida": None}
        estado["alertaReconocidaId"] = ultima.get("id")
        nivel = _nivel(estado)
    print(f"[api] alerta {ultima.get('id')} de {tramo_id} reconocida por el operador")
    return {"tramoId": tramo_id, "nivel": nivel, "reconocida": ultima.get("id")}


@app.post("/tramos/estado", summary="[Stub Balance Volumetrico] Actualizar estado del tramo")
def actualizar_estado_balance(payload: dict = Body(...)):
    tramo_id = payload.get("tramo")
    if not tramo_id:
        raise HTTPException(status_code=400, detail="Falta el campo 'tramo'")
    with lock:
        _asegurar_tramo(tramo_id)
        estado_tramos[tramo_id]["balanceVolumetrico"] = payload
    print(f"[api] [stub balance] estado actualizado para {tramo_id}: {payload}")
    return {"recibido": True}


@app.post("/notificaciones/urgente", summary="[Stub Balance Volumetrico] Notificacion urgente")
def notificacion_urgente(payload: dict = Body(...)):
    print(f"[api] [stub balance] *** NOTIFICACION URGENTE ***: {payload}")
    return {"recibido": True, "canal": "urgente"}
