from regla import EstadoTramo, Nominal, procesar_muestra, severidad_para

NOMINAL = Nominal(presion_bar=45.0, caudal_m3h=120.0)
UMBRALES = dict(umbral_presion_pct=10.0, umbral_balance_m3h=8.0, lecturas_consecutivas=3)


def muestra(estado, presion, arriba, abajo, i):
    estado.caudal_arriba = arriba
    estado.caudal_abajo = abajo
    estado.presion = presion
    return procesar_muestra(estado, "tramo-14", NOMINAL, f"2026-09-18T00:00:{i:02d}+00:00", f"id-{i}", **UMBRALES)


def test_lecturas_normales_no_alertan_y_no_necesitan_linea_base():
    estado = EstadoTramo()
    for i in range(10):
        assert muestra(estado, 45.0, 120.0, 120.0, i) is None
    assert estado.anomalo is False


def test_tramo_que_arranca_anomalo_se_detecta_igual():
    estado = EstadoTramo()
    resultados = [muestra(estado, 35.0, 120.0, 102.0, i) for i in range(3)]
    assert resultados[0] is None and resultados[1] is None
    d = resultados[2]
    assert d is not None
    assert d.caida_pct == 22.22
    assert d.descuadre_m3h == 18.0
    assert d.ids_origen == ["id-0", "id-1", "id-2"]
    assert d.inicio_iso.endswith(":00+00:00") and d.fin_iso.endswith(":02+00:00")


def test_una_alerta_por_episodio_aunque_persista():
    estado = EstadoTramo()
    alertas = [muestra(estado, 35.0, 120.0, 102.0, i) for i in range(20)]
    assert sum(1 for a in alertas if a) == 1


def test_una_lectura_aislada_no_confirma():
    estado = EstadoTramo()
    assert muestra(estado, 35.0, 120.0, 102.0, 0) is None
    assert muestra(estado, 45.0, 120.0, 120.0, 1) is None
    assert muestra(estado, 35.0, 120.0, 102.0, 2) is None
    assert muestra(estado, 35.0, 120.0, 102.0, 3) is None
    assert estado.consecutivas == 2 and estado.anomalo is False


def test_se_exigen_las_dos_senales():
    estado = EstadoTramo()

    for i in range(5):
        assert muestra(estado, 35.0, 120.0, 120.0, i) is None

    for i in range(5, 10):
        assert muestra(estado, 45.0, 120.0, 100.0, i) is None


def test_nuevo_episodio_genera_nueva_alerta():
    estado = EstadoTramo()
    primera = [muestra(estado, 35.0, 120.0, 102.0, i) for i in range(3)][-1]
    assert primera is not None
    assert muestra(estado, 45.0, 120.0, 120.0, 3) is None
    assert estado.anomalo is False
    segunda = [muestra(estado, 35.0, 120.0, 102.0, i) for i in range(4, 7)][-1]
    assert segunda is not None and segunda.ids_origen == ["id-4", "id-5", "id-6"]


def test_no_evalua_sin_el_juego_completo_de_sensores():
    estado = EstadoTramo()
    estado.presion = 35.0
    assert procesar_muestra(estado, "tramo-14", NOMINAL, "t", "id", **UMBRALES) is None
    assert estado.consecutivas == 0


def test_severidad():
    assert severidad_para(10.5, 10.0) == "media"
    assert severidad_para(22.2, 10.0) == "alta"
