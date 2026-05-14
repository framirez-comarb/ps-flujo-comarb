"""Cobertura día a día de las dos custom dims (pantalla y js_ga_sesion_id).
Útil para visualizar el impacto del fix de GTM y separar pre/post deploy.
Sirve también como baseline para cuando se aplique la fix de js_ga_sesion_id."""
import io
import sys
from collections import defaultdict

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
START_DATE = "2026-04-21"
END_DATE = "2026-05-06"

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

creds = Credentials.from_service_account_file(
    r"C:\Users\FARP\Documents\Proyects\Fede4\comarb-analytics-580ca8f5412c.json",
    scopes=SCOPES,
)
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
        if v and v not in ("(not set)", ""):
            by_day[d]["con"] += n
        else:
            by_day[d]["sin"] += n
    return by_day


def imprimir(by_day: dict, titulo: str):
    print(f"\n  ── {titulo} ──")
    print(f"  {'Fecha':<12}{'Día':<5}{'Total':>8}{'Con':>7}{'Sin':>7}{'%OK':>9}")
    print(f"  {'-'*12}{'-'*5}{'-'*8}{'-'*7}{'-'*7}{'-'*9}")
    dias = ["Lu", "Ma", "Mi", "Ju", "Vi", "Sa", "Do"]
    import datetime
    for d in sorted(by_day.keys()):
        s = by_day[d]
        tot = s["con"] + s["sin"]
        pct = round(100 * s["con"] / tot, 1) if tot else 0
        marca = "✓" if pct >= 99 else ("⚠" if pct >= 50 else "✗")
        fecha_dt = datetime.datetime.strptime(d, "%Y%m%d")
        nombre_dia = dias[fecha_dt.weekday()]
        fecha_fmt = fecha_dt.strftime("%Y-%m-%d")
        print(f"  {fecha_fmt:<12}{nombre_dia:<5}{tot:>8}{s['con']:>7}{s['sin']:>7}{pct:>7}% {marca}")


print(f"Período: {START_DATE} → {END_DATE}\n")
print("="*78)
print("  Cobertura día a día de las custom dims post-fix")
print("="*78)

pantalla = query_por_dia("customEvent:pantalla")
sid = query_por_dia("customEvent:js_ga_sesion_id")
imprimir(pantalla, "customEvent:pantalla")
imprimir(sid, "customEvent:js_ga_sesion_id")
