import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

REGISTRY = "http://localhost:18081"
COMPATIBILIDAD = "BACKWARD_TRANSITIVE"
SCHEMAS = Path(__file__).resolve().parent.parent / "schemas"
SUBJECTS = {
    "ot.telemetry.v1-value": SCHEMAS / "telemetria-lectura.schema.json",
    "it.alerts.valve-anomaly.v1-value": SCHEMAS / "alerta-anomalia.schema.json",
}


def llamar(metodo, ruta, cuerpo):
    req = urllib.request.Request(
        REGISTRY + ruta,
        data=json.dumps(cuerpo).encode(),
        method=metodo,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def registrar():
    ok = True
    for subject, ruta in SUBJECTS.items():
        status, resp = llamar("POST", f"/subjects/{subject}/versions",
                              {"schema": ruta.read_text(encoding="utf-8"), "schemaType": "JSON"})
        print(f"{status} registrar {subject}: {resp}")
        ok &= status == 200
        status, resp = llamar("PUT", f"/config/{subject}", {"compatibility": COMPATIBILIDAD})
        print(f"{status} compatibilidad {subject}: {resp}")
        ok &= status == 200
    return ok


def probar_incompatible():
    esquema = json.loads(SUBJECTS["ot.telemetry.v1-value"].read_text(encoding="utf-8"))
    esquema["properties"]["data"]["properties"]["valor"]["type"] = "string"
    status, resp = llamar("POST", "/compatibility/subjects/ot.telemetry.v1-value/versions/latest?verbose=true",
                          {"schema": json.dumps(esquema), "schemaType": "JSON"})
    mensajes = [m for m in resp.get("messages", []) if not str(m).startswith("{oldSchema:")]
    print(f"{status} data.valor number -> string: is_compatible={resp.get('is_compatible')}")
    for m in mensajes:
        print(f"    {m}")
    return resp.get("is_compatible") is False


if __name__ == "__main__":
    try:
        exito = registrar()
        if "--probar" in sys.argv:
            exito &= probar_incompatible()
    except urllib.error.URLError as e:
        print(f"no se pudo conectar al Schema Registry en {REGISTRY}: {e.reason}")
        sys.exit(2)
    sys.exit(0 if exito else 1)
