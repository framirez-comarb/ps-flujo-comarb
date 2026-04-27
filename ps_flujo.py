"""
Presentación Simplificada — Análisis de flujo de usuarios (GA4)
================================================================
Reconstruye el recorrido de cada sesión a través de las 8 pantallas de la
Presentación Simplificada y produce un reporte HTML con:
  - Funnel (sesiones que alcanzan cada paso) + drop-off por paso
  - Errores de validación por pantalla
  - Rutas de escape a versión clásica
  - Caminos de abandono más comunes
Excluye entornos de prueba vía filtro hostName = servicios.comarb.gob.ar.

Uso:
    python analisis_flujo/ps_flujo.py -c comarb-analytics-580ca8f5412c.json
    python analisis_flujo/ps_flujo.py -c ... --desde 2026-01-01 --hasta 2026-04-16
"""

import argparse
import io
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# Forzar UTF-8 en stdout/stderr para consolas Windows (cp1252)
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    FilterExpressionList,
    Metric,
    RunReportRequest,
)
from google.oauth2.service_account import Credentials

# ── Constantes ────────────────────────────────────────────────
PROPERTY_ID = "485388348"
SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
GA4_HOSTNAME = "servicios.comarb.gob.ar"  # excluye localhost + serviciosqa

PAGE_SIZE = 100000  # límite por página; API tope 250k total por query

# Pantallas del flujo (índice 0..7) y evento de "avance" desde cada una.
# El paso 6 (Finalizar DJ) tiene múltiples salidas finales (presentar/guardar).
PASOS = [
    ("Datos de jurisdicciones y actividades", "PS_boton_continuar_0"),
    ("Base Imponible",                        "PS_boton_continuar_1"),
    ("Datos de Facturación",                  "PS_boton_continuar_2"),
    ("Impuesto Determinado",                  "PS_boton_continuar_3"),
    ("Deducciones",                           "PS_boton_continuar_4"),
    ("Débitos y Créditos",                    "PS_boton_continuar_5"),
    ("Finalizar DJ",                          None),
    ("Generar Pago",                          "PS_boton_generar_volante_de_pago"),
]

# Eventos terminales del flujo (alcanzar el último paso efectivo = presentación)
EVENTOS_FINALES_PRESENTACION = {
    "PS_boton_presentar_y_salir",
    "PS_boton_presentar_y_generar_pago",
}
EVENTO_FINAL_PAGO = "PS_boton_generar_volante_de_pago"
EVENTO_GUARDAR_BORRADOR = "PS_boton_guardar_borrador_y_salir"

EVENTOS_ESCAPE = {
    "PS_boton_ir_dj_mensual_normal":                    "versión clásica",
    "PS_boton_ir_dj_mensual_desde_deducciones":         "clásica (desde Deducciones)",
    "PS_boton_ir_dj_mensual_desde_debitos_y_creditos":  "clásica (desde Débitos y Créditos)",
    "PS_boton_ir_listado_ddjj":                         "listado de DDJJ",
}

EVENTOS_VOLVER = {f"PS_boton_volver_{i}" for i in range(1, 7)}

EVENTOS_CONTINUAR = {p[1] for p in PASOS if p[1] and p[1].startswith("PS_boton_continuar_")}

EVENTO_ERROR = "PS_error_validacion_dj"

# Set completo de eventos PS que queremos traer en Query 1
EVENTOS_PS_FLUJO = (
    EVENTOS_CONTINUAR
    | EVENTOS_FINALES_PRESENTACION
    | {EVENTO_FINAL_PAGO, EVENTO_GUARDAR_BORRADOR}
    | set(EVENTOS_ESCAPE.keys())
    | EVENTOS_VOLVER
    | {
        EVENTO_ERROR,
        "PS_boton_enviar_encuesta",
        "PS_cerrar_encuesta",
        "PS_editar_datos_impuesto_determinado",
        "PS_guardar_datos_impuesto_determinado",
        "PS_cancelar_datos_impuesto_determinado",
        "PS_combo_box_seleccionar_tratamiento_fiscal",
        "PS_switch_asistente_ayuda",
    }
)

# Etiquetas legibles (para el HTML)
EVENT_LABELS = {
    "PS_boton_continuar_0": "Continuar (Jurisdicciones)",
    "PS_boton_continuar_1": "Continuar (Base Imponible)",
    "PS_boton_continuar_2": "Continuar (Datos Facturación)",
    "PS_boton_continuar_3": "Continuar (Impuesto Determinado)",
    "PS_boton_continuar_4": "Continuar (Deducciones)",
    "PS_boton_continuar_5": "Continuar (Débitos y Créditos)",
    "PS_boton_presentar_y_salir": "Presentar y Salir",
    "PS_boton_presentar_y_generar_pago": "Presentar y Generar Pago",
    "PS_boton_guardar_borrador_y_salir": "Guardar borrador y salir",
    "PS_boton_generar_volante_de_pago": "Generar Volante de Pago",
    "PS_boton_ir_dj_mensual_normal": "Ir a DJ Mensual (clásica)",
    "PS_boton_ir_dj_mensual_desde_deducciones": "Ir a clásica (desde Deducciones)",
    "PS_boton_ir_dj_mensual_desde_debitos_y_creditos": "Ir a clásica (desde Débitos/Créditos)",
    "PS_boton_ir_listado_ddjj": "Ir al listado de DDJJ",
    "PS_error_validacion_dj": "Error de validación",
    "PS_boton_enviar_encuesta": "Enviar encuesta",
    "PS_cerrar_encuesta": "Cerrar encuesta",
    "PS_editar_datos_impuesto_determinado": "Editar impuesto determinado",
    "PS_guardar_datos_impuesto_determinado": "Guardar impuesto determinado",
    "PS_cancelar_datos_impuesto_determinado": "Cancelar edición impuesto",
    "PS_combo_box_seleccionar_tratamiento_fiscal": "Seleccionar tratamiento fiscal",
    "PS_switch_asistente_ayuda": "Toggle asistente de ayuda",
    "PS_boton_volver_1": "Volver (desde Base Imponible)",
    "PS_boton_volver_2": "Volver (desde Datos Facturación)",
    "PS_boton_volver_3": "Volver (desde Impuesto Determinado)",
    "PS_boton_volver_4": "Volver (desde Deducciones)",
    "PS_boton_volver_5": "Volver (desde Débitos/Créditos)",
    "PS_boton_volver_6": "Volver (desde Finalizar DJ)",
}


# ═══════════════════════════════════════════════════════════════
# Helpers GA4
# ═══════════════════════════════════════════════════════════════

def make_client(creds_path: str) -> BetaAnalyticsDataClient:
    credentials = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return BetaAnalyticsDataClient(credentials=credentials)


def _hostname_filter() -> FilterExpression:
    return FilterExpression(
        filter=Filter(
            field_name="hostName",
            string_filter=Filter.StringFilter(
                value=GA4_HOSTNAME,
                match_type=Filter.StringFilter.MatchType.EXACT,
            ),
        )
    )


def _event_in_filter(events: set[str]) -> FilterExpression:
    return FilterExpression(
        or_group=FilterExpressionList(
            expressions=[
                FilterExpression(
                    filter=Filter(
                        field_name="eventName",
                        string_filter=Filter.StringFilter(
                            value=ev,
                            match_type=Filter.StringFilter.MatchType.EXACT,
                        ),
                    )
                )
                for ev in events
            ]
        )
    )


def _page_path_contains(substr: str) -> FilterExpression:
    return FilterExpression(
        filter=Filter(
            field_name="pagePath",
            string_filter=Filter.StringFilter(
                value=substr,
                match_type=Filter.StringFilter.MatchType.CONTAINS,
            ),
        )
    )


def _run_paginated(
    client: BetaAnalyticsDataClient,
    dimensions: list[Dimension],
    metrics: list[Metric],
    filter_expr: FilterExpression,
    start_date: str,
    end_date: str,
    label: str = "query",
) -> list:
    """Ejecuta una query GA4 con paginación por offset.
    Retorna lista de rows (response.rows concatenados)."""
    all_rows = []
    offset = 0
    page = 0
    while True:
        page += 1
        req = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            dimensions=dimensions,
            metrics=metrics,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=filter_expr,
            limit=PAGE_SIZE,
            offset=offset,
        )
        resp = client.run_report(req)
        n = len(resp.rows)
        all_rows.extend(resp.rows)
        total = resp.row_count if hasattr(resp, "row_count") else None
        print(f"    • {label} página {page}: {n} filas (offset={offset}, total_api={total})")
        if n < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        if offset >= 250_000:
            print(f"    ⚠️  Se alcanzó el tope de 250k rows; los datos pueden estar incompletos.")
            break
    return all_rows


# ═══════════════════════════════════════════════════════════════
# Query 1 — Eventos PS del flujo (evento-por-evento con sessionId)
# ═══════════════════════════════════════════════════════════════

def extract_events(
    client: BetaAnalyticsDataClient,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Trae todos los eventos PS del flujo con eventName + CUIT + exact_timestamp +
    pagePath + texto_del_error.

    NOTA: La GA4 Data API v1beta pública no expone `userPseudoId` ni `sessionId`,
    así que la "sesión" se construye agrupando por (CUIT, fecha-UTC) — el mismo
    criterio que usa ps_verificacion.py para deduplicar. Eventos sin CUIT
    (típicamente por bug de GTM) quedan con CUIT vacío y se agrupan en un
    "bucket" aparte por fecha."""
    filter_expr = FilterExpression(
        and_group=FilterExpressionList(
            expressions=[_hostname_filter(), _event_in_filter(EVENTOS_PS_FLUJO)]
        )
    )
    # Tier C1+C2: agregamos deviceCategory/operatingSystem/browser y métrica
    # userEngagementDuration. Query 1 tiene 8 dimensiones + 1 dim implícita del
    # filtro hostName = 9 total (tope de la API).
    # NOTA: texto_del_error NO va acá para no exceder el tope (bug fix 2026-04-23).
    # Se trae aparte en extract_error_texts() y se mergea.
    dims = [
        Dimension(name="eventName"),
        Dimension(name="customEvent:CUIT"),
        Dimension(name="customEvent:exact_timestamp"),
        Dimension(name="date"),                         # fallback de ordenamiento
        Dimension(name="pagePath"),
        Dimension(name="deviceCategory"),               # Tier C2
        Dimension(name="operatingSystem"),              # Tier C2
        Dimension(name="browser"),                      # Tier C2
    ]
    metrics = [
        Metric(name="eventCount"),
        Metric(name="userEngagementDuration"),          # Tier C1 (en segundos)
    ]

    print("  📡 Query 1: eventos PS (timestamp + CUIT + pagePath + device + engagement)...")
    rows = _run_paginated(
        client, dims, metrics, filter_expr, start_date, end_date, label="eventos",
    )
    dim_names = [
        "event_name", "cuit", "exact_timestamp", "date", "page_path",
        "device_category", "operating_system", "browser",
    ]
    data = []
    for row in rows:
        rec = {name: row.dimension_values[i].value for i, name in enumerate(dim_names)}
        rec["event_count"] = int(row.metric_values[0].value) if row.metric_values else 1
        # userEngagementDuration viene en segundos (como string float)
        try:
            rec["engagement_seg"] = float(row.metric_values[1].value) if len(row.metric_values) > 1 else 0.0
        except (ValueError, TypeError):
            rec["engagement_seg"] = 0.0
        data.append(rec)
    df = pd.DataFrame(data, columns=dim_names + ["event_count", "engagement_seg"])

    # Limpiar (not set) manteniendo CUIT aparte: en CUIT conservamos el marker
    # para el sanity check; en los otros campos sí lo normalizamos.
    df["page_path"] = df["page_path"].replace("(not set)", "")
    # Para device/os/browser mantenemos (not set) como '(desconocido)' para
    # distinguirlo de ausencia de datos en el reporte.
    for col in ["device_category", "operating_system", "browser"]:
        df[col] = df[col].replace("(not set)", "(desconocido)")
    # texto_del_error se populará en main() con merge desde extract_error_texts
    df["texto_del_error"] = ""
    print(f"  ✅ Query 1: {len(df)} filas")
    return df


# ═══════════════════════════════════════════════════════════════
# Query 1b (bug-fix 2026-04-23) — Texto de errores, en query aparte
# ═══════════════════════════════════════════════════════════════

def extract_error_texts(
    client: BetaAnalyticsDataClient,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Trae sólo los eventos PS_error_validacion_dj con texto_del_error.
    Se mergea después contra el DF principal por (cuit, exact_timestamp, date).

    Se hace aparte porque texto_del_error no entra en Query 1 (tope de 9 dims)
    y además sólo es relevante para este evento — no tiene sentido traerlo
    para los otros 25 eventos que siempre tienen el campo vacío."""
    filter_expr = FilterExpression(
        and_group=FilterExpressionList(
            expressions=[
                _hostname_filter(),
                FilterExpression(filter=Filter(
                    field_name="eventName",
                    string_filter=Filter.StringFilter(
                        value=EVENTO_ERROR,
                        match_type=Filter.StringFilter.MatchType.EXACT,
                    ),
                )),
            ]
        )
    )
    dims = [
        Dimension(name="customEvent:CUIT"),
        Dimension(name="customEvent:exact_timestamp"),
        Dimension(name="date"),
        Dimension(name="customEvent:texto_del_error"),
    ]
    metrics = [Metric(name="eventCount")]
    print("  📡 Query 1b: textos de PS_error_validacion_dj...")
    try:
        rows = _run_paginated(
            client, dims, metrics, filter_expr, start_date, end_date, label="errores",
        )
    except Exception as e:
        print(f"  ⚠️  Query 1b falló: {type(e).__name__}: {str(e)[:200]}")
        return pd.DataFrame(columns=["cuit", "exact_timestamp", "date", "texto_del_error"])

    data = []
    for row in rows:
        rec = {
            "cuit":             row.dimension_values[0].value,
            "exact_timestamp":  row.dimension_values[1].value,
            "date":             row.dimension_values[2].value,
            "texto_del_error":  row.dimension_values[3].value,
        }
        data.append(rec)
    df = pd.DataFrame(data, columns=["cuit", "exact_timestamp", "date", "texto_del_error"])
    df["texto_del_error"] = df["texto_del_error"].replace("(not set)", "")
    print(f"  ✅ Query 1b: {len(df)} filas de error")
    return df


# ═══════════════════════════════════════════════════════════════
# Query 1c — js_ga_sesion_id por evento (para clave de sesión real)
# ═══════════════════════════════════════════════════════════════

def extract_session_ids(
    client: BetaAnalyticsDataClient,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Trae customEvent:js_ga_sesion_id junto con las dimensiones mínimas
    para mergearlo con df_eventos por (cuit, exact_timestamp, date).

    Se hace aparte porque Query 1 ya está en el tope de 9 dimensiones (con
    device/OS/browser + pagePath), así que agregar js_ga_sesion_id directo
    no entra. La dimensión custom está registrada en GA4 admin desde el
    22-abril-2026."""
    filter_expr = FilterExpression(
        and_group=FilterExpressionList(
            expressions=[_hostname_filter(), _event_in_filter(EVENTOS_PS_FLUJO)]
        )
    )
    dims = [
        Dimension(name="customEvent:CUIT"),
        Dimension(name="customEvent:exact_timestamp"),
        Dimension(name="date"),
        Dimension(name="customEvent:js_ga_sesion_id"),
    ]
    metrics = [Metric(name="eventCount")]
    print("  📡 Query 1c: session_id por evento...")
    try:
        rows = _run_paginated(
            client, dims, metrics, filter_expr, start_date, end_date, label="session_ids",
        )
    except Exception as e:
        print(f"  ⚠️  Query 1c falló: {type(e).__name__}: {str(e)[:200]}")
        print(f"      Continuamos sin session_id — sesiones caen al fallback (CUIT+fecha)")
        return pd.DataFrame(columns=["cuit", "exact_timestamp", "date", "js_ga_sesion_id"])

    data = []
    for row in rows:
        rec = {
            "cuit":               row.dimension_values[0].value,
            "exact_timestamp":    row.dimension_values[1].value,
            "date":               row.dimension_values[2].value,
            "js_ga_sesion_id":    row.dimension_values[3].value,
        }
        data.append(rec)
    df = pd.DataFrame(data, columns=["cuit", "exact_timestamp", "date", "js_ga_sesion_id"])
    df["js_ga_sesion_id"] = df["js_ga_sesion_id"].replace("(not set)", "")
    n_con_sid = (df["js_ga_sesion_id"] != "").sum()
    print(f"  ✅ Query 1c: {len(df)} filas totales, {n_con_sid} con session_id válido")
    return df


# ═══════════════════════════════════════════════════════════════
# Query 2 — page_view events del flujo
# ═══════════════════════════════════════════════════════════════

def extract_pageviews(
    client: BetaAnalyticsDataClient,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Agrega page_views de /siferesimplificada/ por (pagePath, date).
    Sin userPseudoId/sessionId, sólo podemos contar visitas por pantalla/día —
    útil para comparar cuántos usuarios *entraron* a cada pantalla vs. cuántos
    hicieron click en un botón PS de esa pantalla."""
    filter_expr = FilterExpression(
        and_group=FilterExpressionList(
            expressions=[
                _hostname_filter(),
                FilterExpression(
                    filter=Filter(
                        field_name="eventName",
                        string_filter=Filter.StringFilter(
                            value="page_view",
                            match_type=Filter.StringFilter.MatchType.EXACT,
                        ),
                    )
                ),
                _page_path_contains("siferesimplificada"),
            ]
        )
    )
    dims = [
        Dimension(name="pagePath"),
        Dimension(name="date"),
    ]
    metrics = [Metric(name="eventCount"), Metric(name="totalUsers")]
    print("  📡 Query 2: page_views agregados por (pagePath, date)...")
    try:
        rows = _run_paginated(
            client, dims, metrics, filter_expr, start_date, end_date,
            label="page_views",
        )
    except Exception as e:
        print(f"  ⚠️  Query 2 falló: {type(e).__name__}: {str(e)[:200]}")
        return pd.DataFrame(columns=["page_path", "date", "event_count", "total_users"])

    data = []
    for row in rows:
        rec = {
            "page_path":    row.dimension_values[0].value,
            "date":         row.dimension_values[1].value,
        }
        rec["event_count"] = int(row.metric_values[0].value) if row.metric_values else 0
        rec["total_users"] = int(row.metric_values[1].value) if len(row.metric_values) > 1 else 0
        data.append(rec)
    df = pd.DataFrame(data, columns=["page_path", "date", "event_count", "total_users"])
    print(f"  ✅ Query 2: {len(df)} filas agregadas")
    return df


# ═══════════════════════════════════════════════════════════════
# Query 3 (Tier C3) — Fuente de tráfico agregada
# ═══════════════════════════════════════════════════════════════

def extract_traffic_source(
    client: BetaAnalyticsDataClient,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Agrega sessionSource + sessionDefaultChannelGroup para saber de dónde
    vienen los usuarios que disparan eventos PS. No se cruza con sesiones
    individuales — es sólo un panel de diagnóstico para detectar si una
    campaña/email/link específico está generando sesiones problemáticas."""
    filter_expr = FilterExpression(
        and_group=FilterExpressionList(
            expressions=[_hostname_filter(), _event_in_filter(EVENTOS_PS_FLUJO)]
        )
    )
    dims = [
        Dimension(name="sessionSource"),
        Dimension(name="sessionDefaultChannelGroup"),
        Dimension(name="date"),
    ]
    metrics = [Metric(name="sessions"), Metric(name="eventCount")]
    print("  📡 Query 3: fuentes de tráfico...")
    try:
        rows = _run_paginated(
            client, dims, metrics, filter_expr, start_date, end_date,
            label="traffic",
        )
    except Exception as e:
        print(f"  ⚠️  Query 3 (traffic) falló: {type(e).__name__}: {str(e)[:200]}")
        return pd.DataFrame(columns=["source", "channel_group", "date", "sessions", "event_count"])

    data = []
    for row in rows:
        rec = {
            "source":         row.dimension_values[0].value,
            "channel_group":  row.dimension_values[1].value,
            "date":           row.dimension_values[2].value,
        }
        rec["sessions"] = int(row.metric_values[0].value) if row.metric_values else 0
        rec["event_count"] = int(row.metric_values[1].value) if len(row.metric_values) > 1 else 0
        data.append(rec)
    df = pd.DataFrame(data, columns=["source", "channel_group", "date", "sessions", "event_count"])
    print(f"  ✅ Query 3: {len(df)} combinaciones fuente/canal/día")
    return df


# ═══════════════════════════════════════════════════════════════
# Diagnóstico: qué hostnames aparecen (sin filtro) en los eventos PS
# ═══════════════════════════════════════════════════════════════

def extract_hostnames_diagnostic(
    client: BetaAnalyticsDataClient,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Trae los hostnames que generaron eventos PS en el período, sin
    filtro de host. Se usa sólo para log/sanity check al inicio de la
    corrida — confirma que el filtro de producción está excluyendo los
    entornos de prueba."""
    filter_expr = _event_in_filter(EVENTOS_PS_FLUJO)
    dims = [Dimension(name="hostName")]
    metrics = [Metric(name="eventCount")]
    req = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        dimensions=dims,
        metrics=metrics,
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimension_filter=filter_expr,
        limit=100,
    )
    try:
        resp = client.run_report(req)
    except Exception as e:
        print(f"  ⚠️  Diagnóstico hostnames falló: {e}")
        return pd.DataFrame(columns=["hostname", "event_count"])
    data = [
        {"hostname": r.dimension_values[0].value, "event_count": int(r.metric_values[0].value)}
        for r in resp.rows
    ]
    return pd.DataFrame(data)


# ═══════════════════════════════════════════════════════════════
# Reconstrucción de sesiones
# ═══════════════════════════════════════════════════════════════

def _timestamp_to_dt(s: str):
    """Parsea customEvent:exact_timestamp robustamente."""
    if not s:
        return None
    try:
        return pd.to_datetime(s, errors="coerce")
    except Exception:
        return None


def _dhm_to_dt(s: str):
    """dateHourMinute = YYYYMMDDHHMM."""
    if not s or len(s) < 12:
        return None
    try:
        return datetime.strptime(s[:12], "%Y%m%d%H%M")
    except Exception:
        return None


def _paso_desde_evento(event_name: str) -> int | None:
    """Índice del paso en el que el usuario está CUANDO dispara este evento.
    - PS_boton_continuar_N → está EN el paso N, y al hacer click avanza a N+1.
    - PS_boton_volver_N → está en pasoN (volviendo hacia pasoN-1).
    - Eventos finales (presentar_*, guardar_borrador_y_salir) → paso 6 (Finalizar DJ).
    - PS_boton_generar_volante_de_pago → paso 7 (Generar Pago).
    - Eventos de impuesto determinado (editar/guardar/cancelar, tratamiento fiscal) → paso 3.
    - escape desde deducciones → paso 4; desde débitos/créditos → paso 5.
    - PS_error_validacion_dj: no se mapea acá; se infiere del evento previo."""
    if event_name in EVENTOS_CONTINUAR:
        return int(event_name.rsplit("_", 1)[-1])  # 0..5
    if event_name in EVENTOS_VOLVER:
        return int(event_name.rsplit("_", 1)[-1])  # 1..6
    if event_name in EVENTOS_FINALES_PRESENTACION or event_name == EVENTO_GUARDAR_BORRADOR:
        return 6  # Finalizar DJ
    if event_name == EVENTO_FINAL_PAGO:
        return 7  # Generar Pago
    if event_name in {
        "PS_editar_datos_impuesto_determinado",
        "PS_guardar_datos_impuesto_determinado",
        "PS_cancelar_datos_impuesto_determinado",
        "PS_combo_box_seleccionar_tratamiento_fiscal",
    }:
        return 3
    if event_name == "PS_boton_ir_dj_mensual_desde_deducciones":
        return 4
    if event_name == "PS_boton_ir_dj_mensual_desde_debitos_y_creditos":
        return 5
    # PS_boton_ir_dj_mensual_normal, PS_boton_ir_listado_ddjj, PS_switch_asistente_ayuda,
    # PS_boton_enviar_encuesta, PS_cerrar_encuesta — no se pueden mapear a un paso fijo
    return None


def _clasificar_estado_final(secuencia: list[str], paso_max: int) -> str:
    """Clasifica la sesión por el evento terminal MÁS AVANZADO que se haya
    disparado en toda la secuencia (no sólo el último).

    Tier C4: los escapes a versión clásica ahora se clasifican según desde
    qué pantalla se disparó el escape, en vez de un genérico "escapó_a_clásica".

    Motivo: después de presentar, los usuarios a menudo interactúan con la
    encuesta (enviar_encuesta/cerrar_encuesta) y eso quedaría como "último
    evento", ocultando que la sesión sí completó."""
    if not secuencia:
        return "sólo_visitó"
    # Prioridad: generar_volante > presentar > guardar_borrador > escape > abandono
    if EVENTO_FINAL_PAGO in secuencia:
        return "completó_y_pagó"
    if any(ev in EVENTOS_FINALES_PRESENTACION for ev in secuencia):
        return "completó_y_salió"
    if EVENTO_GUARDAR_BORRADOR in secuencia:
        return "guardó_borrador"
    # Tier C4: desglose de escapes por origen.
    # Primero los específicos (desde_deducciones / desde_debitos_y_creditos) —
    # estos identifican inequívocamente desde qué pantalla se escapó.
    if "PS_boton_ir_dj_mensual_desde_deducciones" in secuencia:
        return "escapó_desde_deducciones"
    if "PS_boton_ir_dj_mensual_desde_debitos_y_creditos" in secuencia:
        return "escapó_desde_debitos_creditos"
    # El "ir_dj_mensual_normal" genérico: inferimos desde qué pantalla según
    # el paso_max alcanzado al momento del escape.
    if "PS_boton_ir_dj_mensual_normal" in secuencia:
        if paso_max >= 7:
            return "escapó_desde_generar_pago"
        if paso_max == 6:
            return "escapó_desde_finalizar_dj"
        if paso_max >= 0:
            return f"escapó_desde_paso{paso_max}"
        return "escapó_a_clásica"
    # "Ir al listado de DDJJ" — salida al listado, no es escape propiamente.
    if "PS_boton_ir_listado_ddjj" in secuencia:
        return "salió_al_listado"
    return f"abandonó_en_paso{paso_max}"


# ═══════════════════════════════════════════════════════════════
# Clasificador de campo_con_error desde texto_del_error
# ═══════════════════════════════════════════════════════════════
#
# Como el frontend no puede agregar `campo_con_error` como parámetro del
# evento (Tier A2 rechazado por el dev del sitio), derivamos el código de
# campo desde el `texto_del_error` usando substring matching. Los mensajes
# del validador son estables (vienen del código del sistema, no input de
# usuario), así que este mapeo cubre 100% de los textos observados hasta
# hoy. Cualquier texto que no matchee cae en 'otros' y se lista aparte
# en el reporte para detectar patrones nuevos y sumarlos al dict.
#
# Ventaja vs Tier A2:
# - No requiere coordinación con dev de frontend.
# - Aplica retroactivamente a datos históricos (no hace falta redisparar).
# - Iteración barata: agregar/editar una tupla del dict y re-correr.

CAMPO_ERROR_PATTERNS: list[tuple[str, str]] = [
    # (substring case-insensitive, código de campo normalizado)
    # Orden importa: los más específicos primero (los substrings más largos
    # cubren los más cortos en el caso de que haya overlap).
    ("total distribuido debe ser igual",        "impuesto_determinado.total_distribuido"),
    ("problemas para obtener sus deducciones",  "deducciones.carga_automatica"),
    ("total de ingresos no gravados",           "datos_facturacion.ingresos_no_gravados"),
    ("debe seleccionar un medio de pago",       "generar_pago.medio_de_pago"),
    ("falla al validar datos de firmantes",     "finalizar_dj.firmantes"),
    ("falla al validar total de bases",         "base_imponible.total"),
    ("todas las jurisdicciones deben tener",    "base_imponible.jurisdicciones"),
    ("no se encuentra en estado borrador",      "finalizar_dj.estado_ddjj"),
    ("2 dígitos decimales",                     "formato.decimales"),
    ("2 digitos decimales",                     "formato.decimales"),  # sin tilde
]


def _clasificar_campo_error(texto: str) -> str:
    """Deriva un código estable de campo desde el texto del error.
    Retorna 'otros' si no matchea ningún patrón conocido, '(sin_texto)' si
    el texto está vacío."""
    if not texto or texto == "(not set)":
        return "(sin_texto)"
    t = texto.lower()
    for pattern, campo in CAMPO_ERROR_PATTERNS:
        if pattern in t:
            return campo
    return "otros"


def _paso_del_error(secuencia_con_pasos: list[tuple[str, int | None]], idx_error: int) -> int | None:
    """Dado el índice de un PS_error_validacion_dj dentro de la secuencia,
    retorna el paso inferido del evento previo que pudo haber causado el error.
    Si no hay evento previo con paso asignable, retorna None."""
    for i in range(idx_error - 1, -1, -1):
        _, p = secuencia_con_pasos[i]
        if p is not None:
            return p
    return None


def _moda_o_primero(series: pd.Series) -> str:
    """Valor más frecuente de una Series (ignora vacíos y '(desconocido)').
    Si no hay, retorna '(desconocido)'."""
    s = series.fillna("(desconocido)").astype(str)
    s = s[(s != "") & (s != "(desconocido)")]
    if s.empty:
        return "(desconocido)"
    vc = s.value_counts()
    return str(vc.index[0])


def build_sessions(df_eventos: pd.DataFrame) -> pd.DataFrame:
    """Reconstruye una fila por sesión. Estrategia de agrupación (prioridad):

    1. **`js_ga_sesion_id`** (preferida) — parámetro custom del GTM desde
       22-abril-2026. Cada valor identifica unívocamente una sesión real
       independientemente de CUIT y fecha. Formato: `SES-YYYYMMDD-HHMMSS-NNNNN`.
       Permite distinguir dos sesiones del mismo contribuyente el mismo día, y
       agrupar eventos sin CUIT (ej. `PS_cerrar_encuesta` fantasma) con la
       sesión real a la que pertenecen.
    2. **`(CUIT, fecha)`** (fallback histórico) — para eventos de antes del
       22-abril que no tienen session_id, se usa el mismo criterio que
       ps_verificacion.py (dedup por día por contribuyente).
    3. **Singleton** (último recurso) — eventos sin session_id ni CUIT válido
       se tratan como incidentes aislados (una fila por evento)."""
    if df_eventos.empty:
        return pd.DataFrame()

    df = df_eventos.copy()

    # 1) Parsear exact_timestamp donde venga válido (precisión ms)
    ts_series = df["exact_timestamp"].apply(_timestamp_to_dt)
    ts_series = pd.to_datetime(ts_series, errors="coerce")

    # 2) Fallback a `date` para filas sin exact_timestamp válido
    mask_nat = ts_series.isna()
    if mask_nat.any() and "date" in df.columns:
        fallback = pd.to_datetime(
            df.loc[mask_nat, "date"].astype(str), format="%Y%m%d", errors="coerce"
        )
        fallback = fallback + pd.Timedelta(hours=12)
        ts_series.loc[mask_nat] = fallback

    df["ts"] = ts_series
    df = df[df["ts"].notna()].copy()
    if df.empty:
        return pd.DataFrame()

    # Normalizar session_id, CUIT y fecha
    if "js_ga_sesion_id" not in df.columns:
        df["js_ga_sesion_id"] = ""
    df["sid_norm"] = df["js_ga_sesion_id"].fillna("").astype(str).str.strip()
    df.loc[df["sid_norm"].isin(["(not set)", "nan", "NaN"]), "sid_norm"] = ""

    df["cuit_norm"] = df["cuit"].fillna("").astype(str).str.strip()
    df.loc[df["cuit_norm"].isin(["(not set)", "", "nan", "NaN"]), "cuit_norm"] = "_sin_cuit_"

    df["fecha"] = df["ts"].dt.date.astype(str)
    df = df.reset_index(drop=True)

    # Vectorizado: construir clave de sesión con prioridad SID > CF > SINGLE
    # Default a SINGLE por índice de fila (uno por evento)
    session_key = "SINGLE::" + df.index.astype(str)
    # Si hay CUIT válido, cae en grupo (cuit, fecha) — sobrescribe SINGLE
    mask_cuit = df["cuit_norm"] != "_sin_cuit_"
    session_key = session_key.where(~mask_cuit,
        "CF::" + df["cuit_norm"] + "::" + df["fecha"])
    # Si hay session_id válido, prioridad máxima — sobrescribe CF y SINGLE
    mask_sid = df["sid_norm"] != ""
    session_key = session_key.where(~mask_sid, "SID::" + df["sid_norm"])
    df["_session_key"] = session_key

    df = df.sort_values(["_session_key", "ts"]).reset_index(drop=True)

    # Estadísticas de agrupación
    n_keys = df["_session_key"].nunique()
    n_sid = int((df["_session_key"].str.startswith("SID::")).any() and
                df.loc[df["_session_key"].str.startswith("SID::"), "_session_key"].nunique())
    n_cf = int((df["_session_key"].str.startswith("CF::")).any() and
               df.loc[df["_session_key"].str.startswith("CF::"), "_session_key"].nunique())
    n_single = int((df["_session_key"].str.startswith("SINGLE::")).any() and
                   df.loc[df["_session_key"].str.startswith("SINGLE::"), "_session_key"].nunique())
    print(f"  🔑 Agrupación: {n_keys} sesiones = {n_sid} (SID) + {n_cf} (CUIT+fecha) + {n_single} (singleton)")

    sesiones = []
    for session_key_val, grupo in df.groupby("_session_key", sort=False):
        # Determinar tipo y metadata de la sesión
        if session_key_val.startswith("SID::"):
            session_key_type = "SID"
            session_id_val = session_key_val[5:]
            cuits_validos = [c for c in grupo["cuit_norm"].unique()
                             if c and c != "_sin_cuit_"]
            cuit_sesion = cuits_validos[0] if cuits_validos else ""
            fecha_sesion = grupo["ts"].min().date().isoformat()
        elif session_key_val.startswith("CF::"):
            session_key_type = "CF"
            _, cuit_sesion, fecha_sesion = session_key_val.split("::", 2)
            session_id_val = ""
        else:  # SINGLE::idx
            session_key_type = "SINGLE"
            session_id_val = ""
            cuit_sesion = ""
            fecha_sesion = grupo["ts"].iloc[0].date().isoformat()

        secuencia: list[str] = []
        secuencia_con_pasos: list[tuple[str, int | None]] = []
        paso_max_alcanzado = -1
        primer_ts = grupo["ts"].min()
        ultimo_ts = grupo["ts"].max()
        page_paths_vistas: list[str] = []
        n_volver = 0
        n_errores = 0
        errores_por_paso: Counter = Counter()
        errores_por_campo: Counter = Counter()
        errores_campo_por_paso: dict = {}
        errores_texto: list[str] = []
        escape_event: str | None = None
        escape_paso: int | None = None

        for _, row in grupo.iterrows():
            ev_name = row["event_name"]
            pp = str(row.get("page_path") or "").strip()
            if pp and (not page_paths_vistas or page_paths_vistas[-1] != pp):
                page_paths_vistas.append(pp)

            secuencia.append(ev_name)
            paso = _paso_desde_evento(ev_name)
            secuencia_con_pasos.append((ev_name, paso))

            if ev_name in EVENTOS_VOLVER:
                n_volver += 1
            if ev_name == EVENTO_ERROR:
                n_errores += 1
                paso_err = _paso_del_error(secuencia_con_pasos, len(secuencia_con_pasos) - 1)
                if paso_err is not None:
                    errores_por_paso[paso_err] += 1
                texto = str(row.get("texto_del_error") or "").strip()
                if texto and texto != "(not set)":
                    errores_texto.append(texto)
                campo = _clasificar_campo_error(texto)
                errores_por_campo[campo] += 1
                if paso_err is not None:
                    errores_campo_por_paso.setdefault(paso_err, {})
                    errores_campo_por_paso[paso_err][campo] = (
                        errores_campo_por_paso[paso_err].get(campo, 0) + 1
                    )

            if ev_name in EVENTOS_CONTINUAR:
                n = int(ev_name.rsplit("_", 1)[-1])
                paso_max_alcanzado = max(paso_max_alcanzado, n + 1)
            elif paso is not None:
                paso_max_alcanzado = max(paso_max_alcanzado, paso)

            if ev_name in EVENTOS_ESCAPE and escape_event is None:
                escape_event = ev_name
                ep = _paso_desde_evento(ev_name)
                escape_paso = ep if ep is not None else paso_max_alcanzado

        if secuencia:
            paso_max_alcanzado = max(paso_max_alcanzado, 0)

        estado_final = _clasificar_estado_final(secuencia, paso_max_alcanzado)
        try:
            duracion_seg = int((ultimo_ts - primer_ts).total_seconds())
        except Exception:
            duracion_seg = 0

        device_cat = _moda_o_primero(grupo.get("device_category", pd.Series(dtype=str)))
        os_name    = _moda_o_primero(grupo.get("operating_system", pd.Series(dtype=str)))
        browser    = _moda_o_primero(grupo.get("browser", pd.Series(dtype=str)))
        engagement_total = float(grupo.get("engagement_seg", pd.Series(dtype=float)).fillna(0.0).sum())

        sesiones.append({
            "session_id":           session_id_val,
            "session_key_type":     session_key_type,
            "cuit":                 cuit_sesion,
            "fecha":                fecha_sesion,
            "primer_ts":            primer_ts,
            "ultimo_ts":            ultimo_ts,
            "duracion_seg":         max(0, duracion_seg),
            "n_eventos":            len(secuencia),
            "paso_max_alcanzado":   paso_max_alcanzado,
            "estado_final":         estado_final,
            "tiene_errores":        n_errores > 0,
            "n_errores":            n_errores,
            "errores_por_paso":     dict(errores_por_paso),
            "errores_por_campo":    dict(errores_por_campo),
            "errores_campo_por_paso": errores_campo_por_paso,
            "errores_texto":        errores_texto,
            "n_volver":             n_volver,
            "secuencia":            secuencia,
            "page_paths":           page_paths_vistas,
            "escape_event":         escape_event or "",
            "escape_paso":          escape_paso if escape_paso is not None else -1,
            "device_category":      device_cat,
            "operating_system":     os_name,
            "browser":              browser,
            "engagement_seg":       engagement_total,
        })

    df_out = pd.DataFrame(sesiones)
    if df_out.empty:
        return df_out
    df_out = df_out.sort_values("ultimo_ts", ascending=False).reset_index(drop=True)
    return df_out


# ═══════════════════════════════════════════════════════════════
# Funnel
# ═══════════════════════════════════════════════════════════════

def build_funnel(df_sesiones: pd.DataFrame) -> pd.DataFrame:
    """Calcula conteos por paso + drop-off + errores + volver + escape.

    La semántica de 'paso_max_alcanzado' (ver build_sessions):
      -1 → no alcanzó ningún paso con PS_boton_continuar (nunca hizo click)
       0 → vio la pantalla 0 (inicial) pero no avanzó
       N → alcanzó la pantalla N (después de click en continuar_{N-1})
       6 → llegó a Finalizar DJ (vía continuar_5 o final directo)
       7 → llegó a Generar Pago (vía generar_volante_de_pago)
    """
    rows = []
    # Paso 0 = "Datos de jurisdicciones y actividades" (la pantalla inicial)
    # Una sesión "llegó al paso N" si paso_max_alcanzado >= N.
    for i, (nombre, _) in enumerate(PASOS):
        sub = df_sesiones[df_sesiones["paso_max_alcanzado"] >= i]
        llegaron = int(len(sub))
        # Errores que ocurrieron EN este paso
        errores_en = sum(
            s.get(i, 0) for s in df_sesiones["errores_por_paso"]
        )
        # Sesiones con al menos un PS_boton_volver_N (N == i)
        volver_ev = f"PS_boton_volver_{i}"
        volver_desde = int(
            df_sesiones["secuencia"].apply(lambda seq: volver_ev in seq).sum()
        )
        # Escapes DESDE este paso
        escapes_desde = int(
            ((df_sesiones["escape_paso"] == i) & (df_sesiones["escape_event"] != "")).sum()
        )
        # Tier C1: engagement promedio de sesiones que alcanzaron este paso
        if "engagement_seg" in df_sesiones.columns and llegaron > 0:
            engagement_prom = round(float(sub["engagement_seg"].fillna(0.0).mean()), 1)
        else:
            engagement_prom = 0.0
        rows.append({
            "paso":                  i,
            "pantalla":              nombre,
            "llegaron":              llegaron,
            "errores":               int(errores_en),
            "volver":                volver_desde,
            "escape":                escapes_desde,
            "engagement_promedio_s": engagement_prom,
        })

    df = pd.DataFrame(rows)
    # drop_off_pct = % de usuarios que llegaron al paso anterior pero NO llegaron a éste
    df["drop_off_pct"] = 0.0
    for i in range(1, len(df)):
        prev = df.loc[i - 1, "llegaron"]
        curr = df.loc[i, "llegaron"]
        if prev > 0:
            df.loc[i, "drop_off_pct"] = round(100 * (prev - curr) / prev, 1)
    return df


# ═══════════════════════════════════════════════════════════════
# Sanity check CUIT en eventos finales
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# Caminos de abandono más comunes
# ═══════════════════════════════════════════════════════════════

def top_paths(df_sesiones: pd.DataFrame, solo_abandonos: bool = True, top_n: int = 20) -> list[tuple[str, int]]:
    """Top-N secuencias (representadas como 'ev1 → ev2 → ...').
    Si solo_abandonos=True, excluye sesiones que completaron (presentar/pagar)."""
    if df_sesiones.empty:
        return []
    df = df_sesiones
    if solo_abandonos:
        df = df[~df["estado_final"].isin(["completó_y_pagó", "completó_y_salió"])]
    counter: Counter = Counter()
    for seq in df["secuencia"]:
        if not seq:
            continue
        key = " → ".join(seq)
        counter[key] += 1
    return counter.most_common(top_n)


def top_error_texts_por_paso(df_sesiones: pd.DataFrame) -> dict[int, list[tuple[str, int]]]:
    """Por cada paso, top-5 textos de error más frecuentes.
    Como los textos están en df_sesiones['errores_texto'] como lista, no por paso,
    necesitamos reconstruir paso-por-texto vía la secuencia. Acá hacemos una
    aproximación: si la sesión tuvo UN sólo error, todos los textos se atribuyen
    al paso en `errores_por_paso`. Si tuvo múltiples, agrupamos por paso con
    el texto disponible (pierde mapping exacto para sesiones complejas)."""
    resultado: dict[int, Counter] = defaultdict(Counter)
    for _, s in df_sesiones.iterrows():
        err_por_paso = s.get("errores_por_paso") or {}
        textos = s.get("errores_texto") or []
        if not err_por_paso or not textos:
            continue
        # Distribuir textos entre pasos proporcionalmente (aprox: mismo texto a todos)
        for paso, n in err_por_paso.items():
            for t in textos:
                resultado[paso][t] += n
    # Transformar a lista top-5 por paso
    return {k: v.most_common(5) for k, v in resultado.items()}


# ═══════════════════════════════════════════════════════════════
# HTML Report
# ═══════════════════════════════════════════════════════════════

def _html_escape(s) -> str:
    if s is None:
        return ""
    s = str(s)
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
    )


def generate_report(
    df_sesiones: pd.DataFrame,
    df_funnel: pd.DataFrame,
    df_hostnames_diag: pd.DataFrame,
    df_traffic: pd.DataFrame,
    start_date: str,
    end_date: str,
) -> str:
    """Genera el HTML del reporte de análisis de flujo."""
    generated = datetime.now(tz=ZoneInfo("America/Argentina/Buenos_Aires")).strftime(
        "%d/%m/%Y %H:%M (ART)"
    )

    total_sesiones = len(df_sesiones)
    total_con_eventos = int((df_sesiones["n_eventos"] > 0).sum()) if not df_sesiones.empty else 0
    completaron = 0
    escaparon = 0
    abandonaron = 0
    solo_visitaron = 0
    guardaron_borrador = 0
    salio_al_listado = 0
    if not df_sesiones.empty:
        vc = df_sesiones["estado_final"].value_counts()
        completaron = int(vc.get("completó_y_salió", 0) + vc.get("completó_y_pagó", 0))
        guardaron_borrador = int(vc.get("guardó_borrador", 0))
        # Tier C4: escapó_* es familia, sumar todos los estados que empiezan con "escapó_"
        escaparon = int(sum(v for k, v in vc.items() if str(k).startswith("escapó_")))
        # Compat hacia atrás: el estado viejo "escapó_a_clásica" también cuenta
        salio_al_listado = int(vc.get("salió_al_listado", 0))
        solo_visitaron = int(vc.get("sólo_visitó", 0))
        abandonaron = total_sesiones - completaron - escaparon - solo_visitaron - guardaron_borrador - salio_al_listado
    tasa_conversion = (
        round(100 * completaron / total_con_eventos, 1)
        if total_con_eventos else 0.0
    )

    # ── Datos para el funnel (Chart.js) ──
    funnel_labels = json.dumps(df_funnel["pantalla"].tolist())
    funnel_values = json.dumps(df_funnel["llegaron"].astype(int).tolist())
    funnel_dropoff = json.dumps(df_funnel["drop_off_pct"].astype(float).tolist())
    funnel_errores = json.dumps(df_funnel["errores"].astype(int).tolist())
    funnel_volver = json.dumps(df_funnel["volver"].astype(int).tolist())
    funnel_escape = json.dumps(df_funnel["escape"].astype(int).tolist())

    # ── Tabla funnel (HTML) ──
    funnel_rows = ""
    for _, r in df_funnel.iterrows():
        do_txt = f"{r['drop_off_pct']}%" if r["paso"] > 0 else "—"
        do_cls = "dif-neg" if r["drop_off_pct"] > 20 else ("dif-amber" if r["drop_off_pct"] > 10 else "dif-pos")
        eng = r.get("engagement_promedio_s", 0) or 0
        funnel_rows += f"""<tr>
            <td class="mono">{r['paso']}</td>
            <td>{_html_escape(r['pantalla'])}</td>
            <td class="num">{r['llegaron']}</td>
            <td class="num {do_cls}">{do_txt}</td>
            <td class="num">{r['errores']}</td>
            <td class="num">{r['volver']}</td>
            <td class="num">{r['escape']}</td>
            <td class="num">{eng:.1f}s</td>
        </tr>"""

    # ── Diagnóstico de hostnames (sanity check del filtro) ──
    host_rows = ""
    for _, r in df_hostnames_diag.iterrows():
        hostname = r.get("hostname", "")
        es_prod = hostname == GA4_HOSTNAME
        cls_host = "host-prod" if es_prod else "host-test"
        marca = "✓ PROD" if es_prod else "✗ excluido"
        host_rows += (
            f'<tr class="{cls_host}"><td>{_html_escape(hostname or "(vacío)")}</td>'
            f'<td class="num">{r.get("event_count", 0)}</td>'
            f'<td>{marca}</td></tr>'
        )
    if not host_rows:
        host_rows = '<tr><td colspan="3" style="color:var(--text-dim);text-align:center">Sin datos</td></tr>'

    # ── Caminos de abandono ──
    tops_aband = top_paths(df_sesiones, solo_abandonos=True, top_n=20) if not df_sesiones.empty else []
    paths_rows = ""
    for seq_str, cnt in tops_aband:
        # Reemplazar nombres técnicos por etiquetas cortas
        parts = seq_str.split(" → ")
        pretty = " → ".join(EVENT_LABELS.get(p, p) for p in parts)
        paths_rows += (
            f'<tr><td class="num">{cnt}</td>'
            f'<td class="path">{_html_escape(pretty)}</td></tr>'
        )
    if not paths_rows:
        paths_rows = '<tr><td colspan="2" style="color:var(--text-dim);text-align:center">Sin abandonos registrados</td></tr>'

    # ── Tabla "Escapes a versión clásica" ──
    # Tier C4: el estado_final ahora puede ser escapó_desde_* o escapó_a_clásica (compat).
    escapes_counter: Counter = Counter()
    if not df_sesiones.empty:
        mask_escape = df_sesiones["estado_final"].astype(str).str.startswith("escapó_")
        for _, s in df_sesiones[mask_escape].iterrows():
            escapes_counter[(s["escape_paso"], s["escape_event"])] += 1
    escapes_rows = ""
    for (paso, ev), cnt in escapes_counter.most_common():
        pantalla = PASOS[paso][0] if 0 <= paso < len(PASOS) else f"(paso {paso})"
        descripcion = EVENTOS_ESCAPE.get(ev, ev)
        escapes_rows += (
            f'<tr><td class="mono">{paso if paso >= 0 else "—"}</td>'
            f'<td>{_html_escape(pantalla)}</td>'
            f'<td><code>{_html_escape(descripcion)}</code></td>'
            f'<td class="num">{cnt}</td></tr>'
        )
    if not escapes_rows:
        escapes_rows = '<tr><td colspan="4" style="color:var(--text-dim);text-align:center">Sin escapes registrados</td></tr>'

    # ── Tabla detallada de sesiones ──
    sess_rows = ""
    if not df_sesiones.empty:
        df_show = df_sesiones.head(2000).copy()  # cap para el HTML
        for _, s in df_show.iterrows():
            estado = s["estado_final"]
            # Tier C4: mapeo que soporta estado granular de escape
            if estado in ("completó_y_salió", "completó_y_pagó"):
                estado_cls = "estado-ok"
            elif estado == "guardó_borrador" or estado == "sólo_visitó" or estado == "salió_al_listado":
                estado_cls = "estado-warn"
            elif str(estado).startswith("escapó_"):
                estado_cls = "estado-warn"
            else:
                # abandonó_en_pasoN
                estado_cls = "estado-bad"
            secuencia_str = " → ".join(s["secuencia"]) if s["secuencia"] else "(sin eventos PS)"
            secuencia_pretty = " → ".join(
                EVENT_LABELS.get(x, x) for x in (s["secuencia"] or [])
            ) or "(sin eventos PS)"
            ts = s["ultimo_ts"].strftime("%Y-%m-%d %H:%M:%S") if pd.notna(s["ultimo_ts"]) else ""
            errores_str = f"{s['n_errores']}" if s["n_errores"] > 0 else ""
            cuit_disp = s["cuit"] or '<span style="color:var(--text-dim)">(sin CUIT)</span>'
            sess_rows += f"""<tr>
                <td class="mono">{_html_escape(ts)}</td>
                <td class="mono">{cuit_disp}</td>
                <td class="num">{s['paso_max_alcanzado']}</td>
                <td class="{estado_cls}">{_html_escape(estado)}</td>
                <td class="num">{s['n_eventos']}</td>
                <td class="num">{errores_str}</td>
                <td class="num">{s['n_volver'] or ''}</td>
                <td class="num">{s['duracion_seg']}s</td>
                <td class="path" title="{_html_escape(secuencia_str)}">{_html_escape(secuencia_pretty)}</td>
            </tr>"""
    if not sess_rows:
        sess_rows = '<tr><td colspan="9" style="color:var(--text-dim);text-align:center">Sin sesiones reconstruidas</td></tr>'

    # ── Serializar todas las sesiones como JS para filtrado dinámico por fecha ──
    # Formato compacto (keys cortas) para minimizar tamaño del HTML.
    # Campos:
    #   d=fecha (YYYY-MM-DD del ultimo_ts), pm=paso_max, st=estado_final,
    #   ne=n_eventos, er=n_errores, nv=n_volver, dr=duracion_seg, eg=engagement_seg,
    #   dc=device_category, os=operating_system, br=browser,
    #   sq=secuencia (string con → separator),
    #   ep=escape_paso, ee=escape_event,
    #   epa=errores_por_paso, epc=errores_por_campo, ecp=errores_campo_por_paso
    sessions_js = []
    if not df_sesiones.empty:
        for _, s in df_sesiones.iterrows():
            # Fecha: usar ultimo_ts; fallback a primer_ts; fallback a fecha del grupo
            ts = s.get("ultimo_ts")
            if pd.notna(ts):
                try:
                    fecha_iso = ts.strftime("%Y-%m-%d")
                except Exception:
                    fecha_iso = str(s.get("fecha", ""))
            else:
                fecha_iso = str(s.get("fecha", ""))
            sessions_js.append({
                "d":   fecha_iso,
                "pm":  int(s.get("paso_max_alcanzado", -1) or 0),
                "st":  str(s.get("estado_final", "")),
                "ne":  int(s.get("n_eventos", 0) or 0),
                "er":  int(s.get("n_errores", 0) or 0),
                "nv":  int(s.get("n_volver", 0) or 0),
                "dr":  int(s.get("duracion_seg", 0) or 0),
                "eg":  float(s.get("engagement_seg", 0.0) or 0.0),
                "dc":  str(s.get("device_category", "(desconocido)")),
                "os":  str(s.get("operating_system", "(desconocido)")),
                "br":  str(s.get("browser", "(desconocido)")),
                "sq":  list(s.get("secuencia") or []),
                "ep":  int(s.get("escape_paso", -1) or -1),
                "ee":  str(s.get("escape_event", "") or ""),
                "epa": {str(k): int(v) for k, v in (s.get("errores_por_paso") or {}).items()},
                "epc": {str(k): int(v) for k, v in (s.get("errores_por_campo") or {}).items()},
                "ecp": {str(k): {str(kk): int(vv) for kk, vv in (v or {}).items()}
                        for k, v in (s.get("errores_campo_por_paso") or {}).items()},
                "et":  list(s.get("errores_texto") or []),
            })
    sessions_js_json = json.dumps(sessions_js, ensure_ascii=False, separators=(",", ":"))

    # Constantes adicionales que el JS necesita para recomputar
    pasos_nombres_json = json.dumps([p[0] for p in PASOS], ensure_ascii=False)
    event_labels_json = json.dumps(EVENT_LABELS, ensure_ascii=False)
    escape_labels_json = json.dumps(EVENTOS_ESCAPE, ensure_ascii=False)

    # ── Tier C2: Segmentación por dispositivo ──
    def _build_device_breakdown_rows(df_ses: pd.DataFrame, group_col: str) -> str:
        """Para una columna (device_category/operating_system/browser), calcula por grupo:
        total_sesiones, % completaron, % tienen_errores, drop-off promedio (100 - %llegan_paso6).
        Retorna HTML <tr>s ordenadas por total_sesiones desc."""
        if df_ses.empty or group_col not in df_ses.columns:
            return '<tr><td colspan="5" style="color:var(--text-dim);text-align:center">Sin datos</td></tr>'
        rows_html = ""
        agg = (
            df_ses.assign(_g=df_ses[group_col].fillna("(desconocido)").astype(str))
                  .groupby("_g", dropna=False)
        )
        stats = []
        for key, sub in agg:
            total = len(sub)
            completaron = int(sub["estado_final"].isin(["completó_y_salió", "completó_y_pagó"]).sum())
            con_error = int(sub["tiene_errores"].sum()) if "tiene_errores" in sub.columns else 0
            llegan_6 = int((sub["paso_max_alcanzado"] >= 6).sum())
            pct_compl = round(100 * completaron / total, 1) if total else 0.0
            pct_err   = round(100 * con_error / total, 1) if total else 0.0
            pct_6     = round(100 * llegan_6 / total, 1) if total else 0.0
            stats.append((key, total, pct_compl, pct_err, pct_6))
        stats.sort(key=lambda x: x[1], reverse=True)
        for key, total, pct_compl, pct_err, pct_6 in stats:
            rows_html += (
                f'<tr><td>{_html_escape(key)}</td>'
                f'<td class="num">{total}</td>'
                f'<td class="num estado-ok">{pct_compl}%</td>'
                f'<td class="num estado-bad">{pct_err}%</td>'
                f'<td class="num">{pct_6}%</td></tr>'
            )
        return rows_html

    device_cat_rows = _build_device_breakdown_rows(df_sesiones, "device_category")
    os_rows         = _build_device_breakdown_rows(df_sesiones, "operating_system")
    browser_rows    = _build_device_breakdown_rows(df_sesiones, "browser")

    # ── Tier C3: Traffic source (top 20 combinaciones fuente/canal) ──
    traffic_rows = ""
    if df_traffic is not None and not df_traffic.empty:
        agg_t = (
            df_traffic.groupby(["source", "channel_group"])
                      .agg(sessions=("sessions", "sum"), events=("event_count", "sum"))
                      .reset_index()
                      .sort_values("sessions", ascending=False)
                      .head(20)
        )
        for _, r in agg_t.iterrows():
            traffic_rows += (
                f'<tr><td>{_html_escape(r["source"])}</td>'
                f'<td>{_html_escape(r["channel_group"])}</td>'
                f'<td class="num">{int(r["sessions"])}</td>'
                f'<td class="num">{int(r["events"])}</td></tr>'
            )
    if not traffic_rows:
        traffic_rows = '<tr><td colspan="4" style="color:var(--text-dim);text-align:center">Sin datos de fuentes de tráfico</td></tr>'

    # ── Tier C-err: Errores por campo (3 vistas) ──
    # Vista 1: top campos — agregamos errores_por_campo de todas las sesiones
    # Vista 2: cross-tab pantalla × campo
    # Vista 3: textos no clasificados (bucket 'otros')
    campo_global: Counter = Counter()
    campo_por_paso: dict[int, Counter] = {}
    otros_textos: Counter = Counter()
    campo_primera_fecha: dict[str, str] = {}
    campo_ultima_fecha: dict[str, str] = {}
    if not df_sesiones.empty:
        for _, s in df_sesiones.iterrows():
            por_campo = s.get("errores_por_campo") or {}
            if not isinstance(por_campo, dict):
                continue
            fecha_str = str(s.get("fecha", ""))
            for campo, n in por_campo.items():
                campo_global[campo] += int(n)
                if campo not in campo_primera_fecha or (fecha_str and fecha_str < campo_primera_fecha[campo]):
                    campo_primera_fecha[campo] = fecha_str
                if campo not in campo_ultima_fecha or (fecha_str and fecha_str > campo_ultima_fecha[campo]):
                    campo_ultima_fecha[campo] = fecha_str
            cross = s.get("errores_campo_por_paso") or {}
            if isinstance(cross, dict):
                for paso, mapping in cross.items():
                    if not isinstance(mapping, dict):
                        continue
                    campo_por_paso.setdefault(int(paso), Counter())
                    for campo, n in mapping.items():
                        campo_por_paso[int(paso)][campo] += int(n)
            # Bucket 'otros' y textos — de los 'errores_texto' que cayeron en 'otros'
            textos = s.get("errores_texto") or []
            if isinstance(textos, list):
                for t in textos:
                    if _clasificar_campo_error(t) == "otros":
                        otros_textos[t] += 1

    total_errores_clasificados = sum(campo_global.values())

    # Vista 1: top campos
    campo_top_rows = ""
    for campo, n in campo_global.most_common():
        pct = round(100 * n / total_errores_clasificados, 1) if total_errores_clasificados else 0
        primera = campo_primera_fecha.get(campo, "—")
        ultima = campo_ultima_fecha.get(campo, "—")
        cls = "estado-bad" if campo == "otros" else ("estado-warn" if campo == "(sin_texto)" else "")
        campo_top_rows += (
            f'<tr><td class="mono"><code class="{cls}">{_html_escape(campo)}</code></td>'
            f'<td class="num">{n}</td>'
            f'<td class="num">{pct}%</td>'
            f'<td class="mono">{primera}</td>'
            f'<td class="mono">{ultima}</td></tr>'
        )
    if not campo_top_rows:
        campo_top_rows = '<tr><td colspan="5" style="color:var(--text-dim);text-align:center">Sin errores en el período</td></tr>'

    # Vista 2: cross-tab pantalla × campo
    # Armamos una matriz con filas = campos, columnas = pasos 0..7
    # Los pasos se ordenan 0..7 fijo
    pasos_header = list(range(8))
    campos_ordenados = [c for c, _ in campo_global.most_common()]
    cross_rows = ""
    for campo in campos_ordenados:
        cells = ""
        for paso in pasos_header:
            v = campo_por_paso.get(paso, Counter()).get(campo, 0)
            if v > 0:
                cells += f'<td class="num">{v}</td>'
            else:
                cells += '<td class="num" style="color:var(--text-dim)">·</td>'
        cls = "estado-bad" if campo == "otros" else ("estado-warn" if campo == "(sin_texto)" else "")
        cross_rows += (
            f'<tr><td class="mono"><code class="{cls}">{_html_escape(campo)}</code></td>{cells}</tr>'
        )
    cross_headers = "".join(f'<th data-col="{i+1}" style="text-align:right">Paso {i}</th>' for i in pasos_header)
    if not cross_rows:
        cross_rows = f'<tr><td colspan="{len(pasos_header)+1}" style="color:var(--text-dim);text-align:center">Sin datos</td></tr>'

    # Vista 3: textos en bucket 'otros' (por si aparecieron textos nuevos no mapeados)
    otros_rows = ""
    for txt, n in otros_textos.most_common(50):
        otros_rows += (
            f'<tr><td class="num">{n}</td>'
            f'<td class="path" style="max-width:800px">{_html_escape(txt)}</td></tr>'
        )
    if not otros_rows:
        otros_rows = '<tr><td colspan="2" style="color:var(--text-dim);text-align:center">✓ No hay textos sin clasificar — todos los errores del período matchean con el diccionario actual</td></tr>'

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Análisis de flujo PS — COMARB</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
    /* Tema CLARO (default) */
    :root {{
        --bg: #f5f6fa; --surface: #ffffff; --surface2: #f0f2f7;
        --border: #dfe3ec; --text: #1a1d27; --text-dim: #6b7280;
        --accent: #4f6ef0; --green: #0d9f6e; --amber: #d97706;
        --red: #dc2e5c; --purple: #7c3aed; --cyan: #0891b2;
        --hover-tint: rgba(79,110,240,0.07);
        --row-border-soft: rgba(223,227,236,0.7);
        --chart-grid: rgba(100,116,139,0.18);
        --chart-text: #4b5563;
        --banner-green-bg: rgba(13,159,110,0.10);
        --banner-red-bg: rgba(220,46,92,0.10);
        --color-scheme: light;
        --radius: 12px;
    }}
    /* Tema OSCURO */
    [data-theme="dark"] {{
        --bg: #0f1117; --surface: #1a1d27; --surface2: #242836;
        --border: #2e3345; --text: #e4e6f0; --text-dim: #8b90a5;
        --accent: #6c8aff; --green: #45d9a8; --amber: #f59e42;
        --red: #ef5678; --purple: #a78bfa; --cyan: #38bdf8;
        --hover-tint: rgba(108,138,255,0.04);
        --row-border-soft: rgba(46,51,69,0.5);
        --chart-grid: #2e3345;
        --chart-text: #8b90a5;
        --banner-green-bg: rgba(69,217,168,0.1);
        --banner-red-bg: rgba(239,86,120,0.1);
        --color-scheme: dark;
    }}
    html {{ color-scheme: var(--color-scheme); }}
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
        font-family: 'DM Sans', sans-serif;
        background: var(--bg); color: var(--text);
        line-height: 1.6; padding: 2rem;
    }}
    .container {{ max-width: 1600px; margin: 0 auto; }}
    header {{
        margin-bottom: 2rem; padding-bottom: 1.5rem;
        border-bottom: 1px solid var(--border);
    }}
    header h1 {{ font-size: 1.5rem; font-weight: 700; color: var(--accent); margin-bottom: .2rem; }}
    .header-row {{
        display: flex; align-items: flex-start; justify-content: space-between; gap: 1rem;
    }}
    .theme-toggle {{
        background: var(--surface); color: var(--text);
        border: 1px solid var(--border); border-radius: 10px;
        padding: .5rem .9rem; cursor: pointer;
        font-family: inherit; font-size: .85rem; font-weight: 500;
        display: inline-flex; align-items: center; gap: .4rem;
        transition: background .2s, color .2s, border-color .2s;
        white-space: nowrap;
    }}
    .theme-toggle:hover {{ color: var(--accent); border-color: var(--accent); }}
    .theme-toggle .theme-icon {{ font-size: 1rem; }}
    header .meta {{ font-size: .8rem; color: var(--text-dim); }}

    .period-filter {{
        display: flex; align-items: center; gap: .6rem;
        margin-top: .9rem; font-size: .85rem; color: var(--text-dim);
        flex-wrap: wrap;
    }}
    .period-filter label {{ font-weight: 600; color: var(--text); }}
    .period-filter input[type="date"] {{
        background: var(--surface); color: var(--text);
        border: 1px solid var(--border); border-radius: 6px;
        padding: .35rem .6rem; font-family: inherit; font-size: .85rem;
    }}
    .period-filter input[type="date"]:focus {{
        outline: none; border-color: var(--accent);
    }}
    .period-filter button {{
        background: var(--surface); color: var(--text-dim);
        border: 1px solid var(--border); border-radius: 6px;
        padding: .35rem .65rem; cursor: pointer; font-size: 1rem;
        line-height: 1;
    }}
    .period-filter button:hover {{ color: var(--accent); border-color: var(--accent); }}
    .period-filter .period-info {{
        margin-left: .5rem; font-size: .8rem; color: var(--text-dim);
    }}

    .banner {{
        padding: .75rem 1rem; border-radius: 8px; margin-bottom: 1.2rem;
        font-size: .9rem; font-weight: 500;
    }}
    .banner-green {{ background: var(--banner-green-bg); border: 1px solid var(--green); color: var(--green); }}
    .banner-red   {{ background: var(--banner-red-bg); border: 1px solid var(--red); color: var(--red); }}

    .kpis {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
        gap: 1rem; margin-bottom: 2rem;
    }}
    .kpi {{
        background: var(--surface); border: 1px solid var(--border);
        border-radius: var(--radius); padding: 1.1rem;
    }}
    .kpi .label {{
        font-size: .72rem; text-transform: uppercase;
        letter-spacing: .06em; color: var(--text-dim); margin-bottom: .3rem;
    }}
    .kpi .value {{
        font-size: 1.7rem; font-weight: 700;
        font-family: 'JetBrains Mono', monospace;
    }}
    .v1 {{ color: var(--accent); }} .v2 {{ color: var(--green); }}
    .v3 {{ color: var(--amber); }} .v4 {{ color: var(--red); }}
    .v5 {{ color: var(--purple); }} .v6 {{ color: var(--cyan); }}

    .section {{ margin-bottom: 2rem; }}
    .section h2 {{
        font-size: 1.05rem; font-weight: 600; margin-bottom: 1rem;
        padding-left: .5rem; border-left: 3px solid var(--accent);
    }}
    .card {{
        background: var(--surface); border: 1px solid var(--border);
        border-radius: var(--radius); padding: 1.2rem; overflow-x: auto;
    }}

    table {{ width: 100%; border-collapse: collapse; font-size: .82rem; }}
    th {{
        text-align: left; padding: .55rem .7rem; font-weight: 600;
        color: var(--text-dim); font-size: .72rem; text-transform: uppercase;
        letter-spacing: .04em; border-bottom: 1px solid var(--border);
        position: sticky; top: 0; background: var(--surface);
    }}
    thead tr:first-child th {{ cursor: pointer; user-select: none; white-space: nowrap; }}
    thead tr:first-child th:hover {{ color: var(--accent); }}
    th .sort-arrow {{ font-size: .65rem; margin-left: .3rem; color: var(--text-dim); opacity: .4; }}
    th.sort-active .sort-arrow {{ opacity: 1; color: var(--accent); }}
    td {{ padding: .5rem .7rem; border-bottom: 1px solid var(--border); vertical-align: top; }}
    td.num {{ text-align: right; font-family: 'JetBrains Mono', monospace; font-size: .78rem; }}
    td.mono {{ font-family: 'JetBrains Mono', monospace; font-size: .75rem; color: var(--accent); }}
    td.path {{ font-size: .75rem; max-width: 560px; overflow: hidden; text-overflow: ellipsis; }}
    code {{ font-family: 'JetBrains Mono', monospace; font-size: .75rem; color: var(--purple); }}
    tr:hover td {{ background: var(--hover-tint); }}
    .dif-pos   {{ color: var(--green); font-weight: 600; }}
    .dif-amber {{ color: var(--amber); font-weight: 600; }}
    .dif-neg   {{ color: var(--red); font-weight: 600; }}
    .estado-ok   {{ color: var(--green); font-weight: 600; }}
    .estado-warn {{ color: var(--amber); font-weight: 600; }}
    .estado-bad  {{ color: var(--red); font-weight: 600; }}
    .host-prod td:first-child {{ color: var(--green); font-weight: 600; }}
    .host-test td:first-child {{ color: var(--amber); }}

    .tabs {{ display: flex; gap: 1.5rem; margin: 1.5rem 0 2rem; border-bottom: 2px solid var(--border); }}
    .tab {{
        padding: .8rem 0; cursor: pointer; font-size: 1rem; font-weight: 600;
        color: var(--text-dim); border-bottom: 3px solid transparent;
        margin-bottom: -2px; transition: color .2s, border-color .2s;
    }}
    .tab:hover {{ color: var(--text); }}
    .tab.active {{ color: var(--accent); border-bottom-color: var(--accent); }}
    .tab-content {{ display: none; }}
    .tab-content.active {{ display: block; }}

    .chart-card {{
        background: var(--surface); border: 1px solid var(--border);
        border-radius: var(--radius); padding: 1.2rem; margin-bottom: 1.5rem;
    }}
    .chart-card h3 {{
        font-size: .85rem; font-weight: 600; color: var(--text-dim);
        margin-bottom: .8rem; text-transform: uppercase; letter-spacing: .04em;
    }}

    footer {{
        margin-top: 3rem; padding-top: 1rem; border-top: 1px solid var(--border);
        font-size: .72rem; color: var(--text-dim); text-align: center;
    }}
</style>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/html2pdf.js@0.10.2/dist/html2pdf.bundle.min.js"></script>
</head>
<body>
<div class="container">

<header>
    <div class="header-row">
        <div>
            <h1>Presentación Simplificada — Análisis de flujo</h1>
            <div class="meta">
                Propiedad GA4: {PROPERTY_ID} · Período: {start_date} → {end_date} · Generado: {generated}
            </div>
            <div class="period-filter">
                <label>Filtrar período:</label>
                <input type="date" id="fecha-desde" value="{start_date}" min="{start_date}" max="{end_date}">
                <span>a</span>
                <input type="date" id="fecha-hasta" value="{end_date}" min="{start_date}" max="{end_date}">
                <button type="button" id="period-reset" title="Restablecer período completo">↺</button>
                <span class="period-info" id="period-info"></span>
            </div>
        </div>
        <div style="display:flex;flex-direction:column;gap:.5rem;align-items:flex-end">
            <button id="pdf-download" class="theme-toggle" aria-label="Descargar PDF" title="Descargar PDF (KPIs y gráficos)">
                <span class="theme-icon">📄</span> Descargar PDF
            </button>
            <button id="theme-toggle" class="theme-toggle" aria-label="Cambiar tema" title="Cambiar tema"></button>
        </div>
    </div>
</header>

<div class="kpis">
    <div class="kpi">
        <div class="label">Sesiones únicas</div>
        <div class="value v1" id="kpi-total">{total_sesiones}</div>
    </div>
    <div class="kpi">
        <div class="label">Con al menos 1 evento PS</div>
        <div class="value v6" id="kpi-con-eventos">{total_con_eventos}</div>
    </div>
    <div class="kpi">
        <div class="label">Completaron</div>
        <div class="value v2" id="kpi-completaron">{completaron}</div>
    </div>
    <div class="kpi">
        <div class="label">Guardaron borrador</div>
        <div class="value v5" id="kpi-borrador">{guardaron_borrador}</div>
    </div>
    <div class="kpi">
        <div class="label">Escaparon a clásica</div>
        <div class="value v3" id="kpi-escaparon">{escaparon}</div>
    </div>
    <div class="kpi">
        <div class="label">Abandonaron</div>
        <div class="value v4" id="kpi-abandonaron">{abandonaron}</div>
    </div>
    <div class="kpi">
        <div class="label">Tasa conversión</div>
        <div class="value v2" id="kpi-conversion">{tasa_conversion}%</div>
    </div>
</div>

<div class="tabs">
    <div class="tab active" onclick="switchTab('funnel')">Funnel por pantalla</div>
    <div class="tab" onclick="switchTab('paths')">Caminos de abandono</div>
    <div class="tab" onclick="switchTab('escapes')">Escapes a clásica</div>
    <div class="tab" onclick="switchTab('device')">Segmentación por dispositivo</div>
    <div class="tab" onclick="switchTab('errcampo')">Errores por campo</div>
    <div class="tab" onclick="switchTab('sessions')">Sesiones (detalle)</div>
</div>

<div id="tab-funnel" class="tab-content active">
    <div class="chart-card">
        <h3>Sesiones que alcanzan cada pantalla</h3>
        <div style="position:relative;height:420px">
            <canvas id="chartFunnel"></canvas>
        </div>
    </div>

    <div class="card">
        <h3 style="font-size:.85rem;color:var(--text-dim);text-transform:uppercase;letter-spacing:.04em;margin-bottom:.8rem">Detalle por paso</h3>
        <table id="tbl-funnel">
            <thead>
                <tr>
                    <th data-col="0">Paso <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="1">Pantalla <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="2">Sesiones llegaron <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="3">Drop-off vs paso anterior <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="4">Errores en paso <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="5">Volver desde paso <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="6">Escapes desde paso <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="7">Engagement prom. <span class="sort-arrow">&#x25B2;</span></th>
                </tr>
            </thead>
            <tbody>{funnel_rows}</tbody>
        </table>
    </div>
</div>

<div id="tab-paths" class="tab-content">
    <div class="card">
        <h3 style="font-size:.85rem;color:var(--text-dim);text-transform:uppercase;letter-spacing:.04em;margin-bottom:.8rem">
            Top 20 caminos de sesiones que NO completaron
        </h3>
        <table id="tbl-paths">
            <thead>
                <tr>
                    <th data-col="0">Sesiones <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="1">Secuencia <span class="sort-arrow">&#x25B2;</span></th>
                </tr>
            </thead>
            <tbody>{paths_rows}</tbody>
        </table>
    </div>
</div>

<div id="tab-escapes" class="tab-content">
    <div class="card">
        <h3 style="font-size:.85rem;color:var(--text-dim);text-transform:uppercase;letter-spacing:.04em;margin-bottom:.8rem">
            Sesiones que abandonaron el flujo simplificado por la versión clásica
        </h3>
        <table id="tbl-escapes">
            <thead>
                <tr>
                    <th data-col="0">Paso <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="1">Pantalla <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="2">Destino <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="3">Sesiones <span class="sort-arrow">&#x25B2;</span></th>
                </tr>
            </thead>
            <tbody>{escapes_rows}</tbody>
        </table>
    </div>
</div>

<div id="tab-device" class="tab-content">
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:1.2rem">
        <div class="card">
            <h3 style="font-size:.85rem;color:var(--text-dim);text-transform:uppercase;letter-spacing:.04em;margin-bottom:.8rem">Por tipo de dispositivo</h3>
            <table id="tbl-device-cat">
                <thead>
                    <tr>
                        <th data-col="0">Dispositivo</th>
                        <th data-col="1">Sesiones</th>
                        <th data-col="2">% completaron</th>
                        <th data-col="3">% con errores</th>
                        <th data-col="4">% llegó a Finalizar DJ</th>
                    </tr>
                </thead>
                <tbody>{device_cat_rows}</tbody>
            </table>
        </div>
        <div class="card">
            <h3 style="font-size:.85rem;color:var(--text-dim);text-transform:uppercase;letter-spacing:.04em;margin-bottom:.8rem">Por sistema operativo</h3>
            <table id="tbl-device-os">
                <thead>
                    <tr>
                        <th data-col="0">OS</th>
                        <th data-col="1">Sesiones</th>
                        <th data-col="2">% completaron</th>
                        <th data-col="3">% con errores</th>
                        <th data-col="4">% llegó a Finalizar DJ</th>
                    </tr>
                </thead>
                <tbody>{os_rows}</tbody>
            </table>
        </div>
        <div class="card">
            <h3 style="font-size:.85rem;color:var(--text-dim);text-transform:uppercase;letter-spacing:.04em;margin-bottom:.8rem">Por browser</h3>
            <table id="tbl-device-browser">
                <thead>
                    <tr>
                        <th data-col="0">Browser</th>
                        <th data-col="1">Sesiones</th>
                        <th data-col="2">% completaron</th>
                        <th data-col="3">% con errores</th>
                        <th data-col="4">% llegó a Finalizar DJ</th>
                    </tr>
                </thead>
                <tbody>{browser_rows}</tbody>
            </table>
        </div>
    </div>
</div>

<div id="tab-errcampo" class="tab-content">
    <div class="card">
        <h3 style="font-size:.85rem;color:var(--text-dim);text-transform:uppercase;letter-spacing:.04em;margin-bottom:.8rem">
            Top campos con errores de validación
        </h3>
        <table id="tbl-errcampo-top">
            <thead>
                <tr>
                    <th data-col="0">Campo</th>
                    <th data-col="1" style="text-align:right">Ocurrencias</th>
                    <th data-col="2" style="text-align:right">% del total</th>
                    <th data-col="3">Primera fecha</th>
                    <th data-col="4">Última fecha</th>
                </tr>
            </thead>
            <tbody>{campo_top_rows}</tbody>
        </table>
    </div>

    <div class="card" style="margin-top:1.2rem">
        <h3 style="font-size:.85rem;color:var(--text-dim);text-transform:uppercase;letter-spacing:.04em;margin-bottom:.8rem">
            Cross-tab: campo × pantalla (dónde se rompe cada campo)
        </h3>
        <table id="tbl-errcampo-cross">
            <thead>
                <tr>
                    <th data-col="0">Campo</th>
                    {cross_headers}
                </tr>
            </thead>
            <tbody>{cross_rows}</tbody>
        </table>
    </div>

    <div class="card" style="margin-top:1.2rem">
        <h3 style="font-size:.85rem;color:var(--text-dim);text-transform:uppercase;letter-spacing:.04em;margin-bottom:.8rem">
            Textos sin clasificar (bucket "otros")
        </h3>
        <table id="tbl-errcampo-otros">
            <thead>
                <tr>
                    <th data-col="0" style="text-align:right">Ocurrencias</th>
                    <th data-col="1">Texto del error</th>
                </tr>
            </thead>
            <tbody>{otros_rows}</tbody>
        </table>
    </div>
</div>

<div id="tab-sessions" class="tab-content">
    <div class="card">
        <h3 style="font-size:.85rem;color:var(--text-dim);text-transform:uppercase;letter-spacing:.04em;margin-bottom:.8rem">
            Detalle por sesión (máx. 2000 filas; orden reciente → antiguo)
        </h3>
        <table id="tbl-sessions">
            <thead>
                <tr>
                    <th data-col="0" class="sort-active" data-dir="desc">Último ts <span class="sort-arrow">&#x25BC;</span></th>
                    <th data-col="1">CUIT <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="2">Paso máx <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="3">Estado final <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="4">N° eventos <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="5">Errores <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="6">Volver <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="7">Duración <span class="sort-arrow">&#x25B2;</span></th>
                    <th data-col="8">Secuencia <span class="sort-arrow">&#x25B2;</span></th>
                </tr>
            </thead>
            <tbody>{sess_rows}</tbody>
        </table>
    </div>
</div>

<footer>
    Análisis de flujo PS · COMARB · Datos: GA4 Data API v1beta
</footer>

</div>
<script>
function switchTab(id) {{
    document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
    document.getElementById('tab-' + id).classList.add('active');
    event.target.classList.add('active');
}}

/* ── Ordenamiento de tablas ── */
document.querySelectorAll('thead tr:first-child th[data-col]').forEach(th => {{
    th.addEventListener('click', function() {{
        const table = this.closest('table');
        const tbody = table.querySelector('tbody');
        const col = parseInt(this.dataset.col);
        const headerRow = this.parentElement;

        const wasActive = this.classList.contains('sort-active');
        const oldDir = this.dataset.dir || 'asc';
        const newDir = wasActive ? (oldDir === 'asc' ? 'desc' : 'asc') : 'asc';

        headerRow.querySelectorAll('th[data-col]').forEach(h => {{
            h.classList.remove('sort-active');
            h.dataset.dir = 'asc';
            const arrow = h.querySelector('.sort-arrow');
            if (arrow) arrow.innerHTML = '&#x25B2;';
        }});

        this.classList.add('sort-active');
        this.dataset.dir = newDir;
        const arrow = this.querySelector('.sort-arrow');
        if (arrow) arrow.innerHTML = newDir === 'asc' ? '&#x25B2;' : '&#x25BC;';

        const rows = Array.from(tbody.querySelectorAll('tr'));
        rows.sort((a, b) => {{
            const aText = a.querySelectorAll('td')[col]?.textContent.trim() || '';
            const bText = b.querySelectorAll('td')[col]?.textContent.trim() || '';
            const aNumRaw = aText.replace('%', '').replace('s', '');
            const bNumRaw = bText.replace('%', '').replace('s', '');
            const aNum = Number(aNumRaw);
            const bNum = Number(bNumRaw);
            const aIsNum = aNumRaw !== '' && !isNaN(aNum);
            const bIsNum = bNumRaw !== '' && !isNaN(bNum);
            let cmp;
            if (aIsNum && bIsNum) cmp = aNum - bNum;
            else cmp = aText.localeCompare(bText, 'es');
            return newDir === 'asc' ? cmp : -cmp;
        }});
        rows.forEach(r => tbody.appendChild(r));
    }});
}});

/* ─────────────────────────────────────────────────────────────
   DATA + CONSTANTES
   ───────────────────────────────────────────────────────────── */
Chart.defaults.font.family = "'DM Sans', sans-serif";

const SESSIONS_ALL = {sessions_js_json};
const PASOS_NOMBRES = {pasos_nombres_json};
const EVENT_LABELS = {event_labels_json};
const ESCAPE_LABELS = {escape_labels_json};

const EVENTO_ERROR = 'PS_error_validacion_dj';
const EVENTO_GUARDAR_BORRADOR = 'PS_boton_guardar_borrador_y_salir';
const EVENTO_FINAL_PAGO = 'PS_boton_generar_volante_de_pago';

const STATES_COMPLETE = new Set(['completó_y_salió', 'completó_y_pagó']);

let SESSIONS = SESSIONS_ALL;        // ventana activa (se actualiza al filtrar)
let _funnelChart = null;

/* ─────────────────────────────────────────────────────────────
   UTILITIES
   ───────────────────────────────────────────────────────────── */
function esc(s) {{
    if (s === null || s === undefined) return '';
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}}
function fmt1(n) {{ return (Math.round(n * 10) / 10).toFixed(1); }}
function fmtPct(n) {{ return fmt1(n) + '%'; }}

function filterByDate(desde, hasta) {{
    if (!desde && !hasta) return SESSIONS_ALL.slice();
    return SESSIONS_ALL.filter(s => {{
        const d = s.d || '';
        if (desde && d < desde) return false;
        if (hasta && d > hasta) return false;
        return true;
    }});
}}

/* ─────────────────────────────────────────────────────────────
   KPIs
   ───────────────────────────────────────────────────────────── */
function renderKPIs(sessions) {{
    const total = sessions.length;
    let con_eventos = 0, completaron = 0, guardaron = 0, escaparon = 0,
        solo_visitaron = 0, salio_listado = 0;
    for (const s of sessions) {{
        if (s.ne > 0) con_eventos++;
        if (STATES_COMPLETE.has(s.st)) completaron++;
        else if (s.st === 'guardó_borrador') guardaron++;
        else if (s.st.startsWith('escapó_')) escaparon++;
        else if (s.st === 'sólo_visitó') solo_visitaron++;
        else if (s.st === 'salió_al_listado') salio_listado++;
    }}
    const abandonaron = total - completaron - escaparon - solo_visitaron - guardaron - salio_listado;
    const tasa = con_eventos ? (100 * completaron / con_eventos) : 0;
    const set = (id, v) => {{ const el = document.getElementById(id); if (el) el.textContent = v; }};
    set('kpi-total', total);
    set('kpi-con-eventos', con_eventos);
    set('kpi-completaron', completaron);
    set('kpi-borrador', guardaron);
    set('kpi-escaparon', escaparon);
    set('kpi-abandonaron', abandonaron);
    set('kpi-conversion', fmt1(tasa) + '%');
}}

/* ─────────────────────────────────────────────────────────────
   FUNNEL TABLE + CHART
   ───────────────────────────────────────────────────────────── */
function computeFunnelRows(sessions) {{
    const rows = [];
    for (let i = 0; i < PASOS_NOMBRES.length; i++) {{
        let llegaron = 0, errores = 0, volver = 0, escape = 0;
        let eng_sum = 0, eng_count = 0;
        const volverEv = 'PS_boton_volver_' + i;
        for (const s of sessions) {{
            if (s.pm >= i) {{
                llegaron++;
                eng_sum += (s.eg || 0);
                eng_count++;
            }}
            if (s.epa && s.epa[i]) errores += s.epa[i];
            if (s.sq && s.sq.indexOf(volverEv) >= 0) volver++;
            if (s.ep === i && s.ee) escape++;
        }}
        const eng = eng_count > 0 ? (eng_sum / eng_count) : 0;
        rows.push({{paso: i, pantalla: PASOS_NOMBRES[i], llegaron, errores, volver, escape, eng}});
    }}
    // drop-off respecto al paso anterior
    for (let i = 0; i < rows.length; i++) {{
        if (i === 0) {{ rows[i].drop_off = 0; continue; }}
        const prev = rows[i - 1].llegaron;
        const curr = rows[i].llegaron;
        rows[i].drop_off = prev > 0 ? (100 * (prev - curr) / prev) : 0;
    }}
    return rows;
}}

function renderFunnelTable(rows) {{
    const tbody = document.querySelector('#tbl-funnel tbody');
    if (!tbody) return;
    let html = '';
    for (const r of rows) {{
        const do_txt = r.paso > 0 ? fmtPct(r.drop_off) : '—';
        const do_cls = r.drop_off > 20 ? 'dif-neg' : (r.drop_off > 10 ? 'dif-amber' : 'dif-pos');
        html += '<tr>' +
            '<td class="mono">' + r.paso + '</td>' +
            '<td>' + esc(r.pantalla) + '</td>' +
            '<td class="num">' + r.llegaron + '</td>' +
            '<td class="num ' + do_cls + '">' + do_txt + '</td>' +
            '<td class="num">' + r.errores + '</td>' +
            '<td class="num">' + r.volver + '</td>' +
            '<td class="num">' + r.escape + '</td>' +
            '<td class="num">' + fmt1(r.eng) + 's</td>' +
            '</tr>';
    }}
    tbody.innerHTML = html;
}}

function renderFunnelChart(rows) {{
    const style = getComputedStyle(document.documentElement);
    const gridColor = style.getPropertyValue('--chart-grid').trim() || '#2e3345';
    const textColor = style.getPropertyValue('--chart-text').trim() || '#8b90a5';
    Chart.defaults.color = textColor;

    const canvas = document.getElementById('chartFunnel');
    if (!canvas) return;
    if (_funnelChart) {{ _funnelChart.destroy(); _funnelChart = null; }}

    const labels = rows.map((r, i) => 'Paso ' + i + ' · ' + r.pantalla);
    const valuesL = rows.map(r => r.llegaron);
    const valuesE = rows.map(r => r.errores);
    const valuesV = rows.map(r => r.volver);
    const valuesEs = rows.map(r => r.escape);
    const dropoffs = rows.map(r => fmt1(r.drop_off));

    _funnelChart = new Chart(canvas, {{
        type: 'bar',
        data: {{
            labels: labels,
            datasets: [
                {{label: 'Sesiones que llegaron', data: valuesL, backgroundColor: '#6c8affcc', borderColor: '#6c8aff', borderWidth: 1, borderRadius: 4}},
                {{label: 'Errores en paso',       data: valuesE, backgroundColor: '#ef5678cc', borderColor: '#ef5678', borderWidth: 1, borderRadius: 4}},
                {{label: 'Back (volver)',         data: valuesV, backgroundColor: '#f59e42cc', borderColor: '#f59e42', borderWidth: 1, borderRadius: 4}},
                {{label: 'Escapes a clásica',     data: valuesEs, backgroundColor: '#a78bfacc', borderColor: '#a78bfa', borderWidth: 1, borderRadius: 4}},
            ],
        }},
        options: {{
            indexAxis: 'y', responsive: true, maintainAspectRatio: false,
            plugins: {{
                legend: {{position: 'top', align: 'end'}},
                tooltip: {{callbacks: {{afterLabel: function(ctx) {{
                    if (ctx.datasetIndex === 0 && ctx.dataIndex > 0)
                        return 'Drop-off vs paso anterior: ' + dropoffs[ctx.dataIndex] + '%';
                }}}}}},
            }},
            scales: {{
                x: {{grid: {{color: gridColor}}, beginAtZero: true, ticks: {{precision: 0}}}},
                y: {{grid: {{display: false}}, ticks: {{font: {{size: 11}}, autoSkip: false}}}},
            }},
        }},
    }});
}}

/* ─────────────────────────────────────────────────────────────
   TOP CAMINOS DE ABANDONO
   ───────────────────────────────────────────────────────────── */
function renderPaths(sessions) {{
    const tbody = document.querySelector('#tbl-paths tbody');
    if (!tbody) return;
    const counter = new Map();
    for (const s of sessions) {{
        if (STATES_COMPLETE.has(s.st)) continue;
        if (!s.sq || s.sq.length === 0) continue;
        const key = s.sq.join(' → ');
        counter.set(key, (counter.get(key) || 0) + 1);
    }}
    const sorted = [...counter.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20);
    let html = '';
    for (const [seq, cnt] of sorted) {{
        const pretty = seq.split(' → ').map(p => EVENT_LABELS[p] || p).join(' → ');
        html += '<tr><td class="num">' + cnt + '</td>' +
                '<td class="path">' + esc(pretty) + '</td></tr>';
    }}
    if (!html) html = '<tr><td colspan="2" style="color:var(--text-dim);text-align:center">Sin abandonos registrados</td></tr>';
    tbody.innerHTML = html;
}}

/* ─────────────────────────────────────────────────────────────
   ESCAPES
   ───────────────────────────────────────────────────────────── */
function renderEscapes(sessions) {{
    const tbody = document.querySelector('#tbl-escapes tbody');
    if (!tbody) return;
    const counter = new Map();
    for (const s of sessions) {{
        if (!s.st.startsWith('escapó_')) continue;
        if (!s.ee) continue;
        const key = s.ep + '::' + s.ee;
        counter.set(key, (counter.get(key) || 0) + 1);
    }}
    const sorted = [...counter.entries()].sort((a, b) => b[1] - a[1]);
    let html = '';
    for (const [key, cnt] of sorted) {{
        const [paso_s, ev] = key.split('::');
        const paso = parseInt(paso_s);
        const pantalla = (paso >= 0 && paso < PASOS_NOMBRES.length) ? PASOS_NOMBRES[paso] : ('(paso ' + paso + ')');
        const descripcion = ESCAPE_LABELS[ev] || ev;
        html += '<tr><td class="mono">' + (paso >= 0 ? paso : '—') + '</td>' +
                '<td>' + esc(pantalla) + '</td>' +
                '<td><code>' + esc(descripcion) + '</code></td>' +
                '<td class="num">' + cnt + '</td></tr>';
    }}
    if (!html) html = '<tr><td colspan="4" style="color:var(--text-dim);text-align:center">Sin escapes registrados</td></tr>';
    tbody.innerHTML = html;
}}

/* ─────────────────────────────────────────────────────────────
   SEGMENTACIÓN POR DISPOSITIVO
   ───────────────────────────────────────────────────────────── */
function renderDeviceTable(sessions, field, tblId) {{
    const tbody = document.querySelector('#' + tblId + ' tbody');
    if (!tbody) return;
    const groups = new Map();
    for (const s of sessions) {{
        const g = s[field] || '(desconocido)';
        if (!groups.has(g)) groups.set(g, []);
        groups.get(g).push(s);
    }}
    const stats = [];
    for (const [key, subs] of groups.entries()) {{
        const total = subs.length;
        let compl = 0, err = 0, p6 = 0;
        for (const s of subs) {{
            if (STATES_COMPLETE.has(s.st)) compl++;
            if (s.er > 0) err++;
            if (s.pm >= 6) p6++;
        }}
        stats.push({{
            key,
            total,
            pctC: total ? (100 * compl / total) : 0,
            pctE: total ? (100 * err / total) : 0,
            pctP6: total ? (100 * p6 / total) : 0,
        }});
    }}
    stats.sort((a, b) => b.total - a.total);
    let html = '';
    for (const r of stats) {{
        html += '<tr><td>' + esc(r.key) + '</td>' +
                '<td class="num">' + r.total + '</td>' +
                '<td class="num estado-ok">' + fmtPct(r.pctC) + '</td>' +
                '<td class="num estado-bad">' + fmtPct(r.pctE) + '</td>' +
                '<td class="num">' + fmtPct(r.pctP6) + '</td></tr>';
    }}
    if (!html) html = '<tr><td colspan="5" style="color:var(--text-dim);text-align:center">Sin datos</td></tr>';
    tbody.innerHTML = html;
}}

/* ─────────────────────────────────────────────────────────────
   ERRORES POR CAMPO (3 vistas: top, cross-tab, otros)
   ───────────────────────────────────────────────────────────── */
function renderErrCampo(sessions) {{
    // Top
    const counter = new Map();
    const primeraFecha = new Map();
    const ultimaFecha = new Map();
    const cross = new Map();  // paso -> Map<campo, count>
    const otros = new Map();  // texto -> count
    for (const s of sessions) {{
        if (s.epc) {{
            for (const [campo, n] of Object.entries(s.epc)) {{
                counter.set(campo, (counter.get(campo) || 0) + n);
                const d = s.d || '';
                if (d) {{
                    if (!primeraFecha.has(campo) || d < primeraFecha.get(campo)) primeraFecha.set(campo, d);
                    if (!ultimaFecha.has(campo) || d > ultimaFecha.get(campo)) ultimaFecha.set(campo, d);
                }}
            }}
        }}
        if (s.ecp) {{
            for (const [paso_s, mapping] of Object.entries(s.ecp)) {{
                const paso = parseInt(paso_s);
                if (!cross.has(paso)) cross.set(paso, new Map());
                for (const [campo, n] of Object.entries(mapping)) {{
                    const cur = cross.get(paso);
                    cur.set(campo, (cur.get(campo) || 0) + n);
                }}
            }}
        }}
        if (s.et && Array.isArray(s.et)) {{
            for (const texto of s.et) {{
                if (!texto) continue;
                // Detectar si el texto cae en 'otros' duplicando la lógica del clasificador.
                // Más simple: si el campo asignado en epc contiene 'otros' → contarlo.
                // Acá somos conservadores: si epc no tiene ningún campo que matchee, no es 'otros'.
            }}
        }}
    }}
    // Rellenar 'otros' mirando sessions donde epc tenga key "otros"
    for (const s of sessions) {{
        if (!s.epc || !s.epc.otros) continue;
        if (!s.et || !s.et.length) continue;
        for (const t of s.et) {{ otros.set(t, (otros.get(t) || 0) + 1); }}
    }}

    const total = Array.from(counter.values()).reduce((a, b) => a + b, 0);

    // Vista 1: top campos
    const tbodyTop = document.querySelector('#tbl-errcampo-top tbody');
    if (tbodyTop) {{
        const sorted = [...counter.entries()].sort((a, b) => b[1] - a[1]);
        let html = '';
        for (const [campo, n] of sorted) {{
            const pct = total ? (100 * n / total) : 0;
            const primera = primeraFecha.get(campo) || '—';
            const ultima = ultimaFecha.get(campo) || '—';
            const cls = campo === 'otros' ? 'estado-bad' : (campo === '(sin_texto)' ? 'estado-warn' : '');
            html += '<tr><td class="mono"><code class="' + cls + '">' + esc(campo) + '</code></td>' +
                    '<td class="num">' + n + '</td>' +
                    '<td class="num">' + fmtPct(pct) + '</td>' +
                    '<td class="mono">' + esc(primera) + '</td>' +
                    '<td class="mono">' + esc(ultima) + '</td></tr>';
        }}
        if (!html) html = '<tr><td colspan="5" style="color:var(--text-dim);text-align:center">Sin errores en el período</td></tr>';
        tbodyTop.innerHTML = html;
    }}

    // Vista 2: cross-tab
    const tbodyCross = document.querySelector('#tbl-errcampo-cross tbody');
    if (tbodyCross) {{
        const campos = [...counter.entries()].sort((a, b) => b[1] - a[1]).map(x => x[0]);
        let html = '';
        for (const campo of campos) {{
            let cells = '';
            for (let p = 0; p < 8; p++) {{
                const v = cross.has(p) ? (cross.get(p).get(campo) || 0) : 0;
                if (v > 0) cells += '<td class="num">' + v + '</td>';
                else cells += '<td class="num" style="color:var(--text-dim)">·</td>';
            }}
            const cls = campo === 'otros' ? 'estado-bad' : (campo === '(sin_texto)' ? 'estado-warn' : '');
            html += '<tr><td class="mono"><code class="' + cls + '">' + esc(campo) + '</code></td>' + cells + '</tr>';
        }}
        if (!html) html = '<tr><td colspan="9" style="color:var(--text-dim);text-align:center">Sin datos</td></tr>';
        tbodyCross.innerHTML = html;
    }}

    // Vista 3: otros
    const tbodyOtros = document.querySelector('#tbl-errcampo-otros tbody');
    if (tbodyOtros) {{
        const sorted = [...otros.entries()].sort((a, b) => b[1] - a[1]).slice(0, 50);
        let html = '';
        for (const [txt, n] of sorted) {{
            html += '<tr><td class="num">' + n + '</td>' +
                    '<td class="path" style="max-width:800px">' + esc(txt) + '</td></tr>';
        }}
        if (!html) html = '<tr><td colspan="2" style="color:var(--text-dim);text-align:center">✓ No hay textos sin clasificar en el período</td></tr>';
        tbodyOtros.innerHTML = html;
    }}
}}

/* ─────────────────────────────────────────────────────────────
   TABLA DE SESIONES — filtrar visibilidad por fecha
   ───────────────────────────────────────────────────────────── */
function renderSessionsVisibility(desde, hasta) {{
    const rows = document.querySelectorAll('#tbl-sessions tbody tr');
    rows.forEach(row => {{
        const cells = row.querySelectorAll('td');
        if (cells.length === 0) return;
        const fecha = (cells[0].textContent || '').trim().slice(0, 10);
        let show = true;
        if (desde && fecha < desde) show = false;
        if (hasta && fecha > hasta) show = false;
        row.style.display = show ? '' : 'none';
    }});
}}

/* ─────────────────────────────────────────────────────────────
   MAIN: aplicar filtros y re-render de todas las pestañas
   ───────────────────────────────────────────────────────────── */
function updatePeriodInfo(n, desde, hasta) {{
    const el = document.getElementById('period-info');
    if (!el) return;
    if (desde || hasta) {{
        el.textContent = '· ' + n + ' sesiones en el rango';
    }} else {{
        el.textContent = '· ' + n + ' sesiones (período completo)';
    }}
}}

function applyFilters() {{
    const desde = (document.getElementById('fecha-desde') || {{value:''}}).value;
    const hasta = (document.getElementById('fecha-hasta') || {{value:''}}).value;
    const filtered = filterByDate(desde, hasta);
    SESSIONS = filtered;
    renderKPIs(filtered);
    const funnelRows = computeFunnelRows(filtered);
    renderFunnelTable(funnelRows);
    renderFunnelChart(funnelRows);
    renderPaths(filtered);
    renderEscapes(filtered);
    renderDeviceTable(filtered, 'dc', 'tbl-device-cat');
    renderDeviceTable(filtered, 'os', 'tbl-device-os');
    renderDeviceTable(filtered, 'br', 'tbl-device-browser');
    renderErrCampo(filtered);
    renderSessionsVisibility(desde, hasta);
    updatePeriodInfo(filtered.length, desde, hasta);
}}

/* ─────────────────────────────────────────────────────────────
   TOGGLE DE TEMA (claro / oscuro)
   ───────────────────────────────────────────────────────────── */
const THEME_KEY = 'ps_flujo_theme';
const themeBtn = document.getElementById('theme-toggle');

function applyTheme(theme) {{
    const t = (theme === 'dark') ? 'dark' : 'light';
    document.documentElement.setAttribute('data-theme', t);
    if (themeBtn) {{
        themeBtn.innerHTML = (t === 'dark')
            ? '<span class="theme-icon">☀️</span> Modo claro'
            : '<span class="theme-icon">🌙</span> Modo oscuro';
        themeBtn.title = (t === 'dark') ? 'Cambiar a tema claro' : 'Cambiar a tema oscuro';
    }}
    try {{ localStorage.setItem(THEME_KEY, t); }} catch (_) {{ }}
    // Re-render del chart para que tome los nuevos colores de grid/texto
    const rows = computeFunnelRows(SESSIONS);
    renderFunnelChart(rows);
}}

let savedTheme = 'light';
try {{ savedTheme = localStorage.getItem(THEME_KEY) || 'light'; }} catch (_) {{ }}
applyTheme(savedTheme);

if (themeBtn) {{
    themeBtn.addEventListener('click', () => {{
        const current = document.documentElement.getAttribute('data-theme') || 'light';
        applyTheme(current === 'dark' ? 'light' : 'dark');
    }});
}}

/* ─────────────────────────────────────────────────────────────
   DESCARGA DE PDF (vía html2pdf.js)
   Construye un wrapper offscreen con sólo los KPIs + visualizaciones
   y lo rasteriza a PDF. El contenido refleja el rango de fechas
   actualmente filtrado.
   ───────────────────────────────────────────────────────────── */
const pdfBtn = document.getElementById('pdf-download');
if (pdfBtn) {{
    pdfBtn.addEventListener('click', () => {{
        if (typeof html2pdf === 'undefined') {{
            alert('html2pdf.js no cargó. Revisá tu conexión a internet.');
            return;
        }}
        pdfBtn.disabled = true;
        const labelOriginal = pdfBtn.innerHTML;
        pdfBtn.innerHTML = '<span class="theme-icon">⏳</span> Generando…';

        const prevTheme = document.documentElement.getAttribute('data-theme') || 'light';
        if (prevTheme !== 'light') {{
            document.documentElement.setAttribute('data-theme', 'light');
            const rows = computeFunnelRows(SESSIONS);
            renderFunnelChart(rows);
        }}

        setTimeout(() => {{
            const wrapper = buildPdfWrapper();
            document.body.appendChild(wrapper);

            const fechaArchivo = new Date().toISOString().slice(0, 10);
            html2pdf().from(wrapper).set({{
                margin: [10, 10, 12, 10],
                filename: 'ps_flujo_' + fechaArchivo + '.pdf',
                image: {{ type: 'jpeg', quality: 0.95 }},
                html2canvas: {{
                    scale: 2, useCORS: true, backgroundColor: '#ffffff',
                    logging: false, scrollY: 0,
                }},
                jsPDF: {{ unit: 'mm', format: 'a4', orientation: 'portrait' }},
                pagebreak: {{ mode: ['css', 'legacy'], avoid: ['.kpi', '.chart-card', 'tr'] }},
            }}).save().then(() => {{
                wrapper.remove();
                pdfBtn.disabled = false;
                pdfBtn.innerHTML = labelOriginal;
                if (prevTheme !== 'light') {{
                    document.documentElement.setAttribute('data-theme', prevTheme);
                    const rows = computeFunnelRows(SESSIONS);
                    renderFunnelChart(rows);
                }}
            }}).catch((err) => {{
                console.error('Error al generar PDF', err);
                wrapper.remove();
                pdfBtn.disabled = false;
                pdfBtn.innerHTML = labelOriginal;
                if (prevTheme !== 'light') {{
                    document.documentElement.setAttribute('data-theme', prevTheme);
                    const rows = computeFunnelRows(SESSIONS);
                    renderFunnelChart(rows);
                }}
                alert('Error al generar PDF. Revisá la consola.');
            }});
        }}, 350);
    }});
}}

/* Construye el wrapper offscreen para html2pdf.
   Incluye: header simplificado + KPIs + chart funnel + tabla del funnel
   + escapes a clásica + segmentación por dispositivo + top campos errores.
   NO incluye: top caminos, tabla detalle de sesiones, cross-tab errores,
   textos sin clasificar. */
function buildPdfWrapper() {{
    const w = document.createElement('div');
    w.id = 'pdf-wrapper';
    w.style.cssText = (
        'position:fixed; left:-99999px; top:0; ' +
        'width:780px; padding:18px 20px; background:#ffffff; color:#1a1d27; ' +
        'font-family:\\'DM Sans\\', sans-serif; font-size:11pt;'
    );

    // 1) Header
    const headerOriginal = document.querySelector('header');
    if (headerOriginal) {{
        const headerClone = document.createElement('div');
        headerClone.style.cssText = 'border-bottom:1px solid #dfe3ec; padding-bottom:12px; margin-bottom:14px';
        const h1 = document.createElement('h1');
        h1.textContent = 'Presentación Simplificada — Análisis de flujo';
        h1.style.cssText = 'font-size:20pt; font-weight:700; color:#4f6ef0; margin:0 0 4px 0';
        headerClone.appendChild(h1);
        const meta = headerOriginal.querySelector('.meta');
        if (meta) {{
            const metaClone = document.createElement('div');
            metaClone.textContent = meta.textContent.trim();
            metaClone.style.cssText = 'font-size:9pt; color:#6b7280';
            headerClone.appendChild(metaClone);
        }}
        const desde = document.getElementById('fecha-desde');
        const hasta = document.getElementById('fecha-hasta');
        const periodInfo = document.getElementById('period-info');
        if (desde && hasta) {{
            const periodo = document.createElement('div');
            const info = periodInfo ? periodInfo.textContent.trim() : '';
            periodo.textContent = 'Período visualizado: ' + desde.value + ' → ' + hasta.value + ' ' + info;
            periodo.style.cssText = 'font-size:9pt; color:#6b7280; margin-top:3px';
            headerClone.appendChild(periodo);
        }}
        w.appendChild(headerClone);
    }}

    // 2) KPIs
    const kpisOrig = document.querySelector('.kpis');
    if (kpisOrig) w.appendChild(clonePdfBlock(kpisOrig));

    // 3) Chart funnel + tabla del funnel
    const chartCardFunnel = document.querySelector('#tab-funnel .chart-card');
    if (chartCardFunnel) {{
        const cloned = clonePdfBlock(chartCardFunnel);
        const origCanvases = chartCardFunnel.querySelectorAll('canvas');
        const cloneCanvases = cloned.querySelectorAll('canvas');
        origCanvases.forEach((oc, idx) => {{
            const cc = cloneCanvases[idx];
            if (!cc || !oc) return;
            cc.width = oc.width; cc.height = oc.height;
            try {{ cc.getContext('2d').drawImage(oc, 0, 0); }} catch (_) {{ }}
            cc.style.maxHeight = '380px'; cc.style.height = 'auto';
        }});
        w.appendChild(cloned);
    }}
    const tablaFunnel = document.querySelector('#tab-funnel .card');
    if (tablaFunnel) w.appendChild(clonePdfBlock(tablaFunnel));

    // 4) Page break
    const br = document.createElement('div');
    br.style.cssText = 'page-break-before:always; height:0';
    br.className = 'html2pdf__page-break';
    w.appendChild(br);

    // 5) Escapes a clásica
    const cardEscapes = document.querySelector('#tab-escapes .card');
    if (cardEscapes) {{
        const titulo = document.createElement('h2');
        titulo.textContent = 'Escapes a versión clásica';
        titulo.style.cssText = 'font-size:13pt; color:#4f6ef0; margin:8px 0 8px 0; font-weight:700';
        w.appendChild(titulo);
        w.appendChild(clonePdfBlock(cardEscapes));
    }}

    // 6) Segmentación por dispositivo (3 cards en grid)
    const deviceCards = document.querySelectorAll('#tab-device .card');
    if (deviceCards.length > 0) {{
        const titulo = document.createElement('h2');
        titulo.textContent = 'Segmentación por dispositivo';
        titulo.style.cssText = 'font-size:13pt; color:#4f6ef0; margin:14px 0 8px 0; font-weight:700';
        w.appendChild(titulo);
        deviceCards.forEach(card => w.appendChild(clonePdfBlock(card)));
    }}

    // 7) Page break antes de errores por campo
    const br2 = document.createElement('div');
    br2.style.cssText = 'page-break-before:always; height:0';
    br2.className = 'html2pdf__page-break';
    w.appendChild(br2);

    // 8) Top campos con errores (sólo el primer card del tab-errcampo)
    const cardErrTop = document.querySelector('#tab-errcampo .card');
    if (cardErrTop) {{
        const titulo = document.createElement('h2');
        titulo.textContent = 'Top campos con errores de validación';
        titulo.style.cssText = 'font-size:13pt; color:#4f6ef0; margin:8px 0 8px 0; font-weight:700';
        w.appendChild(titulo);
        w.appendChild(clonePdfBlock(cardErrTop));
    }}

    return w;
}}

function clonePdfBlock(node) {{
    const clone = node.cloneNode(true);
    clone.style.marginBottom = '14px';
    clone.style.pageBreakInside = 'avoid';
    return clone;
}}

/* ─────────────────────────────────────────────────────────────
   EVENT LISTENERS de los inputs de fecha
   ───────────────────────────────────────────────────────────── */
const fechaDesdeEl = document.getElementById('fecha-desde');
const fechaHastaEl = document.getElementById('fecha-hasta');
const periodResetEl = document.getElementById('period-reset');
const defaultDesde = fechaDesdeEl ? fechaDesdeEl.value : '';
const defaultHasta = fechaHastaEl ? fechaHastaEl.value : '';

if (fechaDesdeEl) fechaDesdeEl.addEventListener('change', applyFilters);
if (fechaHastaEl) fechaHastaEl.addEventListener('change', applyFilters);
if (periodResetEl) periodResetEl.addEventListener('click', () => {{
    if (fechaDesdeEl) fechaDesdeEl.value = defaultDesde;
    if (fechaHastaEl) fechaHastaEl.value = defaultHasta;
    applyFilters();
}});

/* Render inicial — dispara todo con el rango completo */
applyFilters();
</script>
</body>
</html>"""
    return html


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

def _save_sessions_csv(df_sesiones: pd.DataFrame, path: Path):
    """Serializa df_sesiones a CSV aplanando columnas complejas."""
    if df_sesiones.empty:
        pd.DataFrame().to_csv(path, index=False, encoding="utf-8-sig")
        return
    df = df_sesiones.copy()
    df["secuencia_str"] = df["secuencia"].apply(lambda xs: " → ".join(xs) if xs else "")
    df["page_paths_str"] = df["page_paths"].apply(lambda xs: " → ".join(xs) if xs else "")
    df["errores_por_paso_str"] = df["errores_por_paso"].apply(lambda d: json.dumps(d) if d else "")
    df["errores_texto_str"] = df["errores_texto"].apply(lambda xs: " | ".join(xs) if xs else "")
    # Tier C-err: serializar breakdown por campo
    if "errores_por_campo" in df.columns:
        df["errores_por_campo_str"] = df["errores_por_campo"].apply(
            lambda d: json.dumps(d) if isinstance(d, dict) and d else ""
        )
    else:
        df["errores_por_campo_str"] = ""
    if "errores_campo_por_paso" in df.columns:
        df["errores_campo_por_paso_str"] = df["errores_campo_por_paso"].apply(
            lambda d: json.dumps(d) if isinstance(d, dict) and d else ""
        )
    else:
        df["errores_campo_por_paso_str"] = ""
    cols = [
        "session_id", "session_key_type",
        "cuit", "fecha", "primer_ts", "ultimo_ts",
        "duracion_seg", "engagement_seg",
        "n_eventos", "paso_max_alcanzado", "estado_final",
        "tiene_errores", "n_errores", "errores_por_paso_str", "errores_texto_str",
        "errores_por_campo_str", "errores_campo_por_paso_str",
        "n_volver", "escape_event", "escape_paso",
        "device_category", "operating_system", "browser",
        "secuencia_str", "page_paths_str",
    ]
    # Asegurarse de que todas existan (compat con corridas viejas)
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df[cols].to_csv(path, index=False, encoding="utf-8-sig")


def main():
    parser = argparse.ArgumentParser(
        description="Análisis de flujo — Presentación Simplificada (GA4)"
    )
    parser.add_argument(
        "-c", "--credentials", required=True,
        help="Ruta al JSON de Service Account de GA4",
    )
    parser.add_argument(
        "--desde", default="2026-01-01",
        help="Fecha inicio YYYY-MM-DD (default: 2026-01-01)",
    )
    parser.add_argument(
        "--hasta", default=None,
        help="Fecha fin YYYY-MM-DD (default: hoy)",
    )
    parser.add_argument(
        "-o", "--output", default="analisis_flujo/ps_flujo.html",
        help="Archivo HTML de salida (default: analisis_flujo/ps_flujo.html)",
    )
    parser.add_argument(
        "--skip-pageviews", action="store_true",
        help="No consultar page_views (más rápido; pierde detección de visitas sin click)",
    )

    args = parser.parse_args()

    start_date = args.desde
    end_date = args.hasta or datetime.now(tz=ZoneInfo("America/Argentina/Buenos_Aires")).strftime("%Y-%m-%d")

    print(f"\n{'═' * 68}")
    print(f"  Presentación Simplificada — Análisis de flujo")
    print(f"  Período: {start_date} → {end_date}")
    print(f"  GA4 Property: {PROPERTY_ID}")
    print(f"  Hostname filtrado: {GA4_HOSTNAME}")
    print(f"{'═' * 68}\n")

    out_html = Path(args.output)
    out_dir = out_html.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    base = out_html.stem
    out_sessions_csv = out_dir / f"{base}_sesiones.csv"
    out_funnel_csv = out_dir / f"{base}_funnel.csv"
    out_events_csv = out_dir / f"{base}_eventos_raw.csv"

    client = make_client(args.credentials)

    # ── Diagnóstico: qué hostnames aparecen (sin filtro, para sanity) ──
    print("▶ Diagnóstico: hostnames que generaron eventos PS (sin filtro)")
    df_hosts = extract_hostnames_diagnostic(client, start_date, end_date)
    if df_hosts.empty:
        print("  (sin datos)")
    else:
        for _, r in df_hosts.iterrows():
            marca = "✓" if r["hostname"] == GA4_HOSTNAME else "✗ (excluido por filtro)"
            print(f"    {r['hostname']:<45} {r['event_count']:>8}  {marca}")

    # ── Query 1: eventos PS ──
    print("\n▶ Paso 1: extracción de eventos PS")
    df_eventos = extract_events(client, start_date, end_date)

    # ── Query 1b: textos de errores (aparte por límite de 9 dims en Query 1) ──
    print("\n▶ Paso 1b: extracción de textos de error")
    df_err_texts = extract_error_texts(client, start_date, end_date)

    # Merge de texto_del_error en df_eventos por (cuit, exact_timestamp, date)
    # Sólo aplica a eventos PS_error_validacion_dj
    if not df_err_texts.empty:
        # Dedup: si una misma fila (cuit, ts, date) tiene múltiples textos
        # (raro, pero posible por particiones de eventCount), nos quedamos con
        # la primera aparición.
        df_err_dedup = df_err_texts.drop_duplicates(
            subset=["cuit", "exact_timestamp", "date"], keep="first"
        )
        # Quitamos la columna placeholder texto_del_error vacía y hacemos el merge
        df_eventos = df_eventos.drop(columns=["texto_del_error"], errors="ignore")
        df_eventos = df_eventos.merge(
            df_err_dedup[["cuit", "exact_timestamp", "date", "texto_del_error"]],
            on=["cuit", "exact_timestamp", "date"],
            how="left",
        )
        df_eventos["texto_del_error"] = df_eventos["texto_del_error"].fillna("")
        # Sanity: sólo tiene sentido para eventos PS_error_validacion_dj
        mask_no_err = df_eventos["event_name"] != EVENTO_ERROR
        df_eventos.loc[mask_no_err, "texto_del_error"] = ""
        n_con_texto = (df_eventos["texto_del_error"] != "").sum()
        print(f"  ✅ Merge: {n_con_texto} eventos PS_error_validacion_dj con texto")

    # ── Query 1c: js_ga_sesion_id por evento (clave de sesión real) ──
    print("\n▶ Paso 1c: extracción de session_id")
    df_session_ids = extract_session_ids(client, start_date, end_date)
    if not df_session_ids.empty:
        df_sid_dedup = df_session_ids.drop_duplicates(
            subset=["cuit", "exact_timestamp", "date"], keep="first"
        )
        df_eventos = df_eventos.merge(
            df_sid_dedup[["cuit", "exact_timestamp", "date", "js_ga_sesion_id"]],
            on=["cuit", "exact_timestamp", "date"],
            how="left",
        )
        df_eventos["js_ga_sesion_id"] = df_eventos["js_ga_sesion_id"].fillna("")
        n_con_sid = (df_eventos["js_ga_sesion_id"] != "").sum()
        print(f"  ✅ Merge: {n_con_sid}/{len(df_eventos)} eventos con session_id ({round(100*n_con_sid/len(df_eventos),1) if len(df_eventos) else 0}%)")
    else:
        df_eventos["js_ga_sesion_id"] = ""

    df_eventos.to_csv(out_events_csv, index=False, encoding="utf-8-sig")
    print(f"  💾 {out_events_csv} ({len(df_eventos)} filas)")

    # ── Query 2: page_views (agregados) ──
    if args.skip_pageviews:
        print("\n▶ Paso 2: page_views (skip por flag)")
        df_pv = pd.DataFrame(columns=["page_path", "date", "event_count", "total_users"])
    else:
        print("\n▶ Paso 2: extracción de page_views agregados")
        df_pv = extract_pageviews(client, start_date, end_date)

    # ── Query 3 (Tier C3): fuentes de tráfico ──
    print("\n▶ Paso 3: extracción de fuentes de tráfico")
    df_traffic = extract_traffic_source(client, start_date, end_date)

    # ── Reconstrucción de sesiones (clave = CUIT + fecha) ──
    # Paso 4 queda asignado a esta reconstrucción; el paso 5 es el funnel.
    print("\n▶ Paso 4: reconstrucción de sesiones (CUIT + fecha)")
    df_sesiones = build_sessions(df_eventos)
    print(f"  ✅ {len(df_sesiones)} sesiones reconstruidas")

    if not df_sesiones.empty:
        _save_sessions_csv(df_sesiones, out_sessions_csv)
        print(f"  💾 {out_sessions_csv}")

    # ── Funnel ──
    print("\n▶ Paso 5: cálculo de funnel")
    df_funnel = build_funnel(df_sesiones) if not df_sesiones.empty else pd.DataFrame(columns=[
        "paso", "pantalla", "llegaron", "errores", "volver", "escape", "drop_off_pct", "engagement_promedio_s",
    ])
    if df_funnel.empty:
        df_funnel = pd.DataFrame([
            {"paso": i, "pantalla": p[0], "llegaron": 0, "errores": 0, "volver": 0, "escape": 0, "drop_off_pct": 0.0, "engagement_promedio_s": 0.0}
            for i, p in enumerate(PASOS)
        ])
    df_funnel.to_csv(out_funnel_csv, index=False, encoding="utf-8-sig")
    print(f"  💾 {out_funnel_csv}")
    for _, r in df_funnel.iterrows():
        do = f"{r['drop_off_pct']}%" if r['paso'] > 0 else "—"
        eng = r.get('engagement_promedio_s', 0) or 0
        print(f"    Paso {int(r['paso'])}: {r['pantalla']:<45} llegaron={int(r['llegaron']):>5} drop-off={do:>6} errores={int(r['errores']):>3} volver={int(r['volver']):>3} escape={int(r['escape']):>3} eng_prom={eng:.1f}s")

    # ── HTML ──
    print(f"\n▶ Paso 6: generación del reporte HTML")
    html = generate_report(
        df_sesiones, df_funnel, df_hosts, df_traffic, start_date, end_date
    )
    out_html.write_text(html, encoding="utf-8")
    print(f"  💾 {out_html}")

    print(f"\n{'═' * 68}")
    print(f"  ✅ Listo.")
    print(f"  Reporte: file:///{out_html.resolve().as_posix()}")
    print(f"{'═' * 68}\n")


if __name__ == "__main__":
    main()
