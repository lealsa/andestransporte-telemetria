from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Nominal:
    presion_bar: float
    caudal_m3h: float


@dataclass
class EstadoTramo:
    presion: Optional[float] = None
    caudal_arriba: Optional[float] = None
    caudal_abajo: Optional[float] = None

    consecutivas: int = 0
    anomalo: bool = False

    episodio_inicio_iso: Optional[str] = None
    episodio_ids: list = field(default_factory=list)


@dataclass
class Deteccion:
    tramo_id: str
    caida_pct: float
    descuadre_m3h: float
    inicio_iso: str
    fin_iso: str
    ids_origen: list


def evaluar_muestra(estado: EstadoTramo, caida_pct: float, descuadre_m3h: float,
                    umbral_presion_pct: float, umbral_balance_m3h: float,
                    lecturas_consecutivas: int) -> bool:
    es_anomala = caida_pct >= umbral_presion_pct and descuadre_m3h >= umbral_balance_m3h
    if es_anomala:
        estado.consecutivas += 1
    else:
        estado.consecutivas = 0
    return es_anomala


def procesar_muestra(estado: EstadoTramo, tramo_id: str, nominal: Nominal,
                     tiempo_iso: str, id_evento: str,
                     umbral_presion_pct: float, umbral_balance_m3h: float,
                     lecturas_consecutivas: int) -> Optional[Deteccion]:
    if None in (estado.presion, estado.caudal_arriba, estado.caudal_abajo):
        return None

    caida_pct = 100.0 * (nominal.presion_bar - estado.presion) / nominal.presion_bar
    descuadre = estado.caudal_arriba - estado.caudal_abajo

    es_anomala = evaluar_muestra(estado, caida_pct, descuadre,
                                 umbral_presion_pct, umbral_balance_m3h, lecturas_consecutivas)

    if not es_anomala:
        if estado.anomalo:
            estado.anomalo = False
        estado.episodio_inicio_iso = None
        estado.episodio_ids = []
        return None

    if estado.consecutivas == 1:
        estado.episodio_inicio_iso = tiempo_iso
        estado.episodio_ids = []
    estado.episodio_ids.append(id_evento)

    if estado.anomalo or estado.consecutivas < lecturas_consecutivas:
        return None

    estado.anomalo = True
    return Deteccion(
        tramo_id=tramo_id,
        caida_pct=round(caida_pct, 2),
        descuadre_m3h=round(descuadre, 2),
        inicio_iso=estado.episodio_inicio_iso or tiempo_iso,
        fin_iso=tiempo_iso,
        ids_origen=list(estado.episodio_ids),
    )


def severidad_para(caida_pct: float, umbral_presion_pct: float) -> str:
    return "alta" if caida_pct > umbral_presion_pct * 1.5 else "media"
