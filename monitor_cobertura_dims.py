"""Cobertura día a día de las dos custom dims (pantalla y js_ga_sesion_id).
Útil para visualizar el impacto del fix de GTM y separar pre/post deploy.

Cuando alguna dim cae por debajo del 100% en algún día del período, hace
automáticamente una segunda query y muestra el breakdown por evento + UA de
los registros sin valor — así no hace falta correr una query ad-hoc para
identificar la causa raíz del outlier.
"""
import argparse
import io
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Filter, FilterExpression, FilterExpressionList,
    Metric, RunReportRequest,
)
from google.oauth2.service_account import Credentials

PROPERTY_ID = "485388348"
SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
HOSTNAME = "servicios.comarb.gob.ar"
DEFAULT_DAYS = 14
DEFAULT_CREDS = r"C:\Users\FARP\Documents\Proyects\Fede4\comarb-analytics-580ca8f5412c.json"

EVENTOS_PS = [
    "PS_boton_continuar_0", "PS_boton_continuar_1", "PS_boton_continuar_2",
    "PS_boton_continuar_3", "PS_boton_continuar_4", "PS_boton_continuar_5",
    "PS_boton_volver_1", "PS_boton_volver_2", "PS_boton_volver_3",
    "PS_boton_volver_4", "PS_boton_volver_5", "PS_boton_volver_6",
    "PS_boton_presentar_y_salir", "PS_boton_presentar_y_generar_pago",
    "PS_boton_guardar_borrador_y_salir", "PS_boton_generar_volante_de_pago",
    "PS_boton_ir_dj_mensual_normal",
    "PS_boton_ir_dj_mensual_desde_deducciones",
    "PS_boton_ir_dj_mensual_desde_debitos_y_creditos",
    "PS_boton_ir_listado_ddjj",
    "PS_boton_enviar_encuesta", "PS_cerrar_encuesta",
    "PS_error_validacion_dj",
    "PS_editar_datos_impuesto_determinado",
    "PS_guardar_datos_impuesto_determinado",
    "PS_cancelar_datos_impuesto_determinado",
    "PS_combo_box_seleccionar_tratamiento_fiscal",
    "PS_switch_asistente_ayuda",
]


def parse_args():
    today = date.today()
    default_desde = today - timedelta(days=DEFAULT_DAYS)
    p = argparse.ArgumentParser(
        description="Cobertura día a día de customEvent:pantalla y customEvent:js_ga_sesion_id.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--desde", default=str(default_desde),
                   help="Fecha inicio del período (YYYY-MM-DD).")
    p.add_argument("--hasta", default=str(today),
                   help="Fecha fin del período (YYYY-MM-DD).")
    p.add_argument("-c", "--creds", default=DEFAULT_CREDS,
                   help="Path al JSON de service account de GA4.")
    args = p.parse_args()
    # Validación: --desde debe ser <= --hasta
    try:
        d1 = datetime.strptime(args.desde, "%Y-%m-%d").date()
        d2 = datetime.strptime(args.hasta, "%Y-%m-%d").date()
    except ValueError as e:
        p.error(f"Formato de fecha inválido: {e}")
    if d1 > d2:
        p.error(f"--desde ({args.desde}) no puede ser posterior a --hasta ({args.hasta})")
    return args


ARGS = parse_args()
START_DATE = ARGS.desde
END_DATE = ARGS.hasta

creds = Credentials.from_service_account_file(ARGS.creds, scopes=SCOPES)
client = BetaAnalyticsDataClient(credentials=creds)

filt = FilterExpression(and_group=FilterExpressionList(expressions=[
    FilterExpression(filter=Filter(
        field_name="hostName",
        string_filter=Filter.StringFilter(
            value=HOSTNAME, match_type=Filter.StringFilter.MatchType.EXACT))),
    FilterExpression(or_group=FilterExpressionList(expressions=[
        FilterExpression(filter=Filter(
            field_name="eventName",
            string_filter=Filter.StringFilter(
                value=ev, match_type=Filter.StringFilter.MatchType.EXACT)))
        for ev in EVENTOS_PS])),
]))


def _is_missing(value: str) -> bool:
    return (not value) or value in ("(not set)", "")


def query_por_dia(custom_dim: str) -> dict:
    """Devuelve {fecha_yyyymmdd: {'con': N, 'sin': N}} para la dim dada."""
    req = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        dimensions=[
            Dimension(name="date"),
            Dimension(name=custom_dim),
        ],
        metrics=[Metric(name="eventCount")],
        date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
        dimension_filter=filt,
        limit=10000,
    )
    resp = client.run_report(req)
    by_day = defaultdict(lambda: {"con": 0, "sin": 0})
    for row in resp.rows:
        d = row.dimension_values[0].value
        v = row.dimension_values[1].value
        n = int(row.metric_values[0].value)
        bucket = "sin" if _is_missing(v) else "con"
        by_day[d][bucket] += n
    return by_day


def imprimir(by_day: dict, titulo: str):
    print(f"\n  ── {titulo} ──")
    print(f"  {'Fecha':<12}{'Día':<5}{'Total':>8}{'Con':>7}{'Sin':>7}{'%OK':>9}")
    print(f"  {'-'*12}{'-'*5}{'-'*8}{'-'*7}{'-'*7}{'-'*9}")
    dias = ["Lu", "Ma", "Mi", "Ju", "Vi", "Sa", "Do"]
    for d in sorted(by_day.keys()):
        s = by_day[d]
        tot = s["con"] + s["sin"]
        pct = round(100 * s["con"] / tot, 1) if tot else 0
        marca = "✓" if pct >= 99 else ("⚠" if pct >= 50 else "✗")
        fecha_dt = datetime.strptime(d, "%Y%m%d")
        nombre_dia = dias[fecha_dt.weekday()]
        fecha_fmt = fecha_dt.strftime("%Y-%m-%d")
        print(f"  {fecha_fmt:<12}{nombre_dia:<5}{tot:>8}{s['con']:>7}{s['sin']:>7}{pct:>7}% {marca}")


def breakdown_outlier(custom_dim: str, titulo: str, top_n: int = 5):
    """Si hay eventos sin la dim, hace una query extra y muestra qué eventos
    y combinaciones device/OS/browser los generan. Permite diagnóstico
    inmediato sin tener que correr una query ad-hoc."""
    req = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        dimensions=[
            Dimension(name="eventName"),
            Dimension(name=custom_dim),
            Dimension(name="deviceCategory"),
            Dimension(name="operatingSystem"),
            Dimension(name="browser"),
            Dimension(name="date"),
        ],
        metrics=[Metric(name="eventCount")],
        date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
        dimension_filter=filt,
        limit=10000,
    )
    resp = client.run_report(req)
    por_evento = defaultdict(int)
    por_ua = defaultdict(int)
    por_dia = defaultdict(int)
    total = 0
    for row in resp.rows:
        ev = row.dimension_values[0].value
        v = row.dimension_values[1].value
        if not _is_missing(v):
            continue
        dev = row.dimension_values[2].value
        os = row.dimension_values[3].value
        br = row.dimension_values[4].value
        d = row.dimension_values[5].value
        n = int(row.metric_values[0].value)
        por_evento[ev] += n
        por_ua[f"{dev}/{os}/{br}"] += n
        por_dia[d] += n
        total += n
    if total == 0:
        return  # ya no hay outlier — coincide con tabla principal

    print(f"\n  ⚠ Breakdown del outlier: {titulo} — {total} eventos sin valor en el período")
    print(f"\n    Por evento (top {top_n}):")
    for ev, n in sorted(por_evento.items(), key=lambda x: -x[1])[:top_n]:
        pct = 100 * n / total
        print(f"      {ev:<50}{n:>5}  ({pct:.1f}%)")
    print(f"\n    Por device/OS/browser (top {top_n}):")
    for ua, n in sorted(por_ua.items(), key=lambda x: -x[1])[:top_n]:
        pct = 100 * n / total
        print(f"      {ua:<40}{n:>5}  ({pct:.1f}%)")
    print(f"\n    Días con outlier:")
    for d in sorted(por_dia.keys()):
        fecha_fmt = datetime.strptime(d, "%Y%m%d").strftime("%Y-%m-%d")
        print(f"      {fecha_fmt}   {por_dia[d]} eventos")


print(f"Período: {START_DATE} → {END_DATE}\n")
print("="*78)
print("  Cobertura día a día de las custom dims post-fix")
print("="*78)

pantalla = query_por_dia("customEvent:pantalla")
sid = query_por_dia("customEvent:js_ga_sesion_id")
imprimir(pantalla, "customEvent:pantalla")
imprimir(sid, "customEvent:js_ga_sesion_id")

# Breakdown automático cuando hay outliers
if any(s["sin"] > 0 for s in pantalla.values()):
    breakdown_outlier("customEvent:pantalla", "customEvent:pantalla")
if any(s["sin"] > 0 for s in sid.values()):
    breakdown_outlier("customEvent:js_ga_sesion_id", "customEvent:js_ga_sesion_id")
