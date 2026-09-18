import json
import os
import subprocess
import threading
from collections import deque
from copy import deepcopy
from pathlib import Path
from typing import Optional

import requests
from fastapi import Body, FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles


BACKEND_DIR = Path(__file__).resolve().parent
PANEL_DIR = BACKEND_DIR.parent
PROTOTIPO_DIR = PANEL_DIR.parent
REPO_DIR = PROTOTIPO_DIR.parent
FRONTEND_DIR = PANEL_DIR / "frontend"
CONTROL_DIR = PANEL_DIR / "control"
CONTROL_FILE = CONTROL_DIR / "anomalias.json"

API_BASE_URL = os.getenv("PANEL_API_BASE_URL", "http://localhost:8090")
SCHEMA_REGISTRY_URL = os.getenv("PANEL_SCHEMA_REGISTRY_URL", "http://localhost:18081")
SUBJECT_TELEMETRIA = "ot.telemetry.v1-value"
SUBJECT_ALERTAS = "it.alerts.valve-anomaly.v1-value"
ESQUEMA_TELEMETRIA_PATH = REPO_DIR / "schemas" / "telemetria-lectura.schema.json"
ESQUEMA_ALERTAS_PATH = REPO_DIR / "schemas" / "alerta-anomalia.schema.json"
COMPATIBILIDAD = "BACKWARD_TRANSITIVE"
HEADERS_SR = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

app = FastAPI(title="Panel de Operacion - AndesTransporte Telemetria")


_job_lock = threading.Lock()
_job = {
    "nombre": None,
    "corriendo": False,
    "codigo_salida": None,
    "logs": deque(maxlen=2000),
}


def _agregar_log(linea: str) -> None:
    with _job_lock:
        _job["logs"].append(linea.rstrip("\n"))


def _terminar_job(codigo: int) -> None:
    with _job_lock:
        _job["codigo_salida"] = codigo
        _job["corriendo"] = False


def _ejecutar_en_segundo_plano(nombre: str, comando: list[str], despues=None) -> None:
    _agregar_log(f"$ {' '.join(comando)}  (cwd={PROTOTIPO_DIR})")
    try:
        proceso = subprocess.Popen(
            comando,
            cwd=str(PROTOTIPO_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        for linea in proceso.stdout:
            _agregar_log(linea)
        proceso.wait()
        _agregar_log(f"--- '{nombre}' termino con codigo {proceso.returncode} ---")
        if proceso.returncode == 0 and despues is not None:
            try:
                despues()
            except Exception as exc:
                _agregar_log(f"[panel] paso posterior a '{nombre}' fallo: {exc}")
        _terminar_job(proceso.returncode)
    except FileNotFoundError:
        _agregar_log("ERROR: no se encontro el comando 'docker'. ¿Docker Desktop esta instalado y en el PATH?")
        _terminar_job(-1)
    except Exception as exc:
        _agregar_log(f"ERROR inesperado ejecutando '{nombre}': {exc}")
        _terminar_job(-1)


def _lanzar_comando(nombre: str, comando: list[str], despues=None) -> dict:
    with _job_lock:
        if _job["corriendo"]:
            raise HTTPException(
                status_code=409,
                detail=f"Ya hay un comando en curso ('{_job['nombre']}'). Espera a que termine (ver /api/stack/logs).",
            )
        _job["nombre"] = nombre
        _job["corriendo"] = True
        _job["codigo_salida"] = None
        _job["logs"].clear()
    hilo = threading.Thread(target=_ejecutar_en_segundo_plano, args=(nombre, comando, despues), daemon=True)
    hilo.start()
    return {"iniciado": True, "comando": nombre}


CONTROL_LIMPIO = {"modo": "manual", "tramosConAnomalia": []}


def _limpiar_control(motivo: str) -> None:
    _escribir_control(dict(CONTROL_LIMPIO))
    _agregar_log(f"[panel] archivo de control reiniciado a 'manual sin tramos' ({motivo}): todos los tramos arrancan en operacion normal.")


@app.post("/api/stack/up", summary="Levantar el stack principal (docker compose up -d --build)")
def stack_up():
    respuesta = _lanzar_comando("levantar stack", ["docker", "compose", "up", "-d", "--build"])
    _limpiar_control("al levantar el stack")
    return respuesta


@app.post(
    "/api/stack/reiniciar-deteccion",
    summary="Reiniciar detector y API (~10 s), sin tocar Redpanda ni el simulador",
)


def stack_reiniciar_deteccion():
    respuesta = _lanzar_comando(
        "reiniciar deteccion",
        ["docker", "compose", "restart", "detector", "api"],
        despues=_reconocer_todas_las_alertas,
    )
    _limpiar_control("al reiniciar la deteccion")
    return respuesta


def _reconocer_todas_las_alertas() -> None:
    import time as _time
    for _ in range(30):
        try:
            r = requests.get(f"{API_BASE_URL}/tramos", timeout=2)
            if r.ok:
                break
        except requests.exceptions.RequestException:
            pass
        _time.sleep(1)
    else:
        _agregar_log("[panel] la API no respondio tras el reinicio; las alertas historicas quedaran sin reconocer")
        return
    _time.sleep(2)
    tramos = requests.get(f"{API_BASE_URL}/tramos", timeout=3).json().get("tramos", [])
    for tramo_id in tramos:
        res = _reconocer_alerta_en_api(tramo_id)
        _agregar_log(f"[panel] {tramo_id}: alerta historica reconocida -> nivel {res.get('nivel', '?')}")


@app.post("/api/stack/down", summary="Bajar el stack (docker compose down)")
def stack_down():
    return _lanzar_comando("bajar stack", ["docker", "compose", "down"])


@app.post("/api/stack/camel", summary="Levantar tambien el flujo Camel (perfil 'camel')")
def stack_camel():
    return _lanzar_comando("levantar flujo Camel", ["docker", "compose", "--profile", "camel", "up", "-d", "--build", "camel"])


@app.get("/api/stack/logs", summary="Ultimas lineas de log del comando en curso o del ultimo ejecutado")
def stack_logs():
    with _job_lock:
        return {
            "comando": _job["nombre"],
            "corriendo": _job["corriendo"],
            "codigo_salida": _job["codigo_salida"],
            "logs": list(_job["logs"]),
        }


@app.get("/api/stack/estado", summary="docker compose ps (que contenedores estan arriba)")
def stack_estado():
    try:
        resultado = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            cwd=str(PROTOTIPO_DIR),
            capture_output=True,
            text=True,
            timeout=15,
        )
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="No se encontro el comando 'docker' en esta maquina.")
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="docker compose ps no respondio a tiempo.")

    contenedores = []

    salida = resultado.stdout.strip()
    if salida:
        try:
            parseado = json.loads(salida)
            contenedores = parseado if isinstance(parseado, list) else [parseado]
        except json.JSONDecodeError:
            for linea in salida.splitlines():
                linea = linea.strip()
                if not linea:
                    continue
                try:
                    contenedores.append(json.loads(linea))
                except json.JSONDecodeError:
                    pass
    return {"contenedores": contenedores, "salida_cruda": salida, "stderr": resultado.stderr}


CONTENEDORES = [
    {"nombre": "redpanda", "rol": "broker Kafka"},
    {"nombre": "redpanda-console", "rol": "UI de Kafka"},
    {"nombre": "mosquitto", "rol": "broker MQTT"},
    {"nombre": "simulador-telemetria", "rol": "genera lecturas OT"},
    {"nombre": "puente-ot-it", "rol": "puente MQTT -> Kafka"},
    {"nombre": "detector-anomalias", "rol": "detecta anomalias"},
    {"nombre": "api-estado-tramo", "rol": "API de estado"},
    {"nombre": "camel-integracion-balance", "rol": "flujo Camel (perfil camel)"},
]
_NOMBRES_CONTENEDORES = {c["nombre"] for c in CONTENEDORES}


@app.get("/api/contenedores", summary="Lista de contenedores conocidos del stack")
def contenedores_lista():
    return {"contenedores": CONTENEDORES}


@app.get("/api/contenedores/logs", summary="docker logs --tail N <contenedor>, para la vista de consolas")
def contenedor_logs(nombre: str, tail: int = 80):
    if nombre not in _NOMBRES_CONTENEDORES:
        raise HTTPException(status_code=400, detail=f"Contenedor desconocido: {nombre}")
    tail = max(1, min(tail, 500))
    try:
        resultado = subprocess.run(
            ["docker", "logs", "--tail", str(tail), nombre],
            capture_output=True,
            text=True,
            timeout=8,
        )
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="No se encontro el comando 'docker'.")
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="docker logs no respondio a tiempo.")
    salida = (resultado.stdout or "") + (resultado.stderr or "")
    lineas = salida.splitlines()
    return {"nombre": nombre, "lineas": lineas[-tail:], "codigo_salida": resultado.returncode}


@app.get("/api/status", summary="Estado consolidado de todos los tramos conocidos")
def status():
    try:
        resp_tramos = requests.get(f"{API_BASE_URL}/tramos", timeout=3)
        resp_tramos.raise_for_status()
        tramos_ids = resp_tramos.json().get("tramos", [])
    except requests.exceptions.RequestException:
        return {"api_disponible": False, "tramos": [], "mensaje": "La API (localhost:8090) no responde todavia. ¿Ya levantaste el stack?"}

    tramos = []
    for tramo_id in tramos_ids:
        try:
            r = requests.get(f"{API_BASE_URL}/tramos/{tramo_id}/estado", timeout=3)
            r.raise_for_status()
            tramos.append(r.json())
        except requests.exceptions.RequestException as exc:
            tramos.append({"tramoId": tramo_id, "nivel": "desconocido", "error": str(exc)})

    return {"api_disponible": True, "tramos": tramos}


def _leer_control() -> dict:
    if not CONTROL_FILE.exists():
        return {"modo": "automatico", "tramosConAnomalia": []}
    try:
        return json.loads(CONTROL_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"modo": "automatico", "tramosConAnomalia": []}


def _escribir_control(datos: dict) -> None:
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CONTROL_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(datos, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, CONTROL_FILE)


@app.get("/api/anomalia/estado", summary="Estado actual del control manual de anomalias")
def anomalia_estado():
    return _leer_control()


@app.post("/api/anomalia/activar", summary="Forzar la anomalia en un tramo especifico, sin esperar el temporizador")
def anomalia_activar(payload: dict = Body(..., example={"tramoId": "tramo-14"})):
    tramo_id = payload.get("tramoId")
    if not tramo_id:
        raise HTTPException(status_code=400, detail="Falta 'tramoId'")
    control = _leer_control()
    control["modo"] = "manual"
    tramos = set(control.get("tramosConAnomalia", []))
    tramos.add(tramo_id)
    control["tramosConAnomalia"] = sorted(tramos)
    _escribir_control(control)
    return control


def _reconocer_alerta_en_api(tramo_id: str) -> dict:
    try:
        r = requests.post(f"{API_BASE_URL}/tramos/{tramo_id}/alertas/reconocer", timeout=3)
        return r.json() if r.ok else {"error": r.text, "status_code": r.status_code}
    except requests.exceptions.RequestException as exc:
        return {"error": str(exc)}


@app.post(
    "/api/anomalia/desactivar",
    summary="Quitar la anomalia de un tramo (vuelve a lecturas normales) y reconocer su alerta en la API (vuelve a 'normal')",
)


def anomalia_desactivar(payload: dict = Body(..., example={"tramoId": "tramo-14"})):
    tramo_id = payload.get("tramoId")
    if not tramo_id:
        raise HTTPException(status_code=400, detail="Falta 'tramoId'")
    control = _leer_control()
    control["modo"] = "manual"
    tramos = set(control.get("tramosConAnomalia", []))
    tramos.discard(tramo_id)
    control["tramosConAnomalia"] = sorted(tramos)
    _escribir_control(control)

    control["reconocimiento"] = _reconocer_alerta_en_api(tramo_id)
    return control


@app.post("/api/anomalia/reconocer", summary="Solo reconocer la alerta del tramo en la API, sin tocar el simulador")
def anomalia_reconocer(payload: dict = Body(..., example={"tramoId": "tramo-14"})):
    tramo_id = payload.get("tramoId")
    if not tramo_id:
        raise HTTPException(status_code=400, detail="Falta 'tramoId'")
    return _reconocer_alerta_en_api(tramo_id)


@app.post("/api/anomalia/reset", summary="Volver al modo automatico original (temporizador global, sin este panel)")
def anomalia_reset():
    if CONTROL_FILE.exists():
        CONTROL_FILE.unlink()
    return {"modo": "automatico", "tramosConAnomalia": []}


def _json_o_texto(r: requests.Response):
    try:
        return r.json()
    except ValueError:
        return {"raw": r.text}


@app.post(
    "/api/esquema/registrar",
    summary="registrar ambos esquemas y fijar BACKWARD_TRANSITIVE",
)


def esquema_registrar():
    pasos = []
    for subject, ruta in ((SUBJECT_TELEMETRIA, ESQUEMA_TELEMETRIA_PATH), (SUBJECT_ALERTAS, ESQUEMA_ALERTAS_PATH)):
        if not ruta.exists():
            raise HTTPException(status_code=500, detail=f"No se encontro {ruta}")
        try:
            r = requests.post(
                f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
                json={"schema": ruta.read_text(encoding="utf-8"), "schemaType": "JSON"},
                headers=HEADERS_SR,
                timeout=5,
            )
            pasos.append({"paso": f"registrar {subject}", "http_status": r.status_code, "respuesta": _json_o_texto(r)})
            r = requests.put(
                f"{SCHEMA_REGISTRY_URL}/config/{subject}",
                json={"compatibility": COMPATIBILIDAD},
                headers=HEADERS_SR,
                timeout=5,
            )
            pasos.append({"paso": f"compatibilidad {subject}", "http_status": r.status_code, "respuesta": _json_o_texto(r)})
        except requests.exceptions.RequestException as exc:
            raise HTTPException(status_code=502, detail=f"No se pudo contactar al Schema Registry en {SCHEMA_REGISTRY_URL}: {exc}")

    todo_ok = all(200 <= p["http_status"] < 300 for p in pasos)
    return {
        "ok": todo_ok,
        "mensaje": (
            f"Ambos subjects registrados con {COMPATIBILIDAD}. Ya se puede probar el cambio incompatible."
            if todo_ok else "Algun paso fallo; revisa 'pasos'."
        ),
        "pasos": pasos,
    }


@app.get("/api/esquema/estado", summary="Regla de compatibilidad configurada en el Schema Registry")
def esquema_estado():
    salida = {}
    for subject in (SUBJECT_TELEMETRIA, SUBJECT_ALERTAS):
        try:
            r = requests.get(f"{SCHEMA_REGISTRY_URL}/config/{subject}", timeout=3)
            salida[subject] = r.json() if r.ok else {"error": r.text, "status_code": r.status_code}
        except requests.exceptions.RequestException as exc:
            salida[subject] = {"error": str(exc)}
    return salida


@app.post(
    "/api/esquema/probar-incompatible",
    summary="probar en vivo que el registry RECHAZA un cambio incompatible (valor: number -> string)",
)


def esquema_probar_incompatible():
    if not ESQUEMA_TELEMETRIA_PATH.exists():
        raise HTTPException(status_code=500, detail=f"No se encontro {ESQUEMA_TELEMETRIA_PATH}")

    esquema_original = json.loads(ESQUEMA_TELEMETRIA_PATH.read_text(encoding="utf-8"))
    esquema_incompatible = deepcopy(esquema_original)
    try:
        esquema_incompatible["properties"]["data"]["properties"]["valor"]["type"] = "string"
    except KeyError as exc:
        raise HTTPException(status_code=500, detail=f"No se encontro el campo esperado en el esquema: {exc}")

    url = f"{SCHEMA_REGISTRY_URL}/compatibility/subjects/{SUBJECT_TELEMETRIA}/versions/latest?verbose=true"

    body = {"schema": json.dumps(esquema_incompatible), "schemaType": "JSON"}

    try:
        r = requests.post(url, json=body, headers=HEADERS_SR, timeout=5)
    except requests.exceptions.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"No se pudo contactar al Schema Registry en {SCHEMA_REGISTRY_URL}: {exc}")

    respuesta_json = _json_o_texto(r)

    if isinstance(respuesta_json, dict) and isinstance(respuesta_json.get("messages"), list):
        respuesta_json["messages"] = [m for m in respuesta_json["messages"] if not str(m).startswith("{oldSchema:")]

    es_compatible = respuesta_json.get("is_compatible")
    if es_compatible is False:
        mensaje = (
            "El Schema Registry RECHAZO el cambio incompatible (valor: number -> string), "
            "como exige BACKWARD_TRANSITIVE."
        )
    elif es_compatible is True:
        mensaje = (
            "ATENCION: el registry dijo que SI es compatible. Revisa que la regla "
            "BACKWARD_TRANSITIVE este configurada (ver /api/esquema/estado) antes de la sustentacion."
        )
    elif r.status_code == 404:
        mensaje = (
            "El subject no existe en el registry (tipico tras `docker compose down -v`). "
            "Pulsa 'Registrar esquemas' primero y repite la prueba."
        )
    else:
        mensaje = "El registry no devolvio 'is_compatible'. Revisa 'respuesta_cruda' para el detalle del error."

    return {
        "cambio_probado": "data.valor: number -> string",
        "http_status": r.status_code,
        "respuesta_cruda": respuesta_json,
        "mensaje": mensaje,
    }


@app.get("/api/health")
def health():
    return {"status": "ok"}


app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=os.getenv("PANEL_HOST", "127.0.0.1"), port=int(os.getenv("PANEL_PORT", "9500")))
