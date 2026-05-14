"""Quick check: ¿en qué forma exacta llega el js_ga_sesion_id ausente?
GA4 distingue '' (vacío) de '(not set)' (no presente). Querer saber cuál
de los dos llega informa qué tan literal hay que ser en el fix GTM."""
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
END_DATE = "2026-05-04"
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
]

creds = Credentials.from_service_account_file(
    r"G:\Otros ordenadores\Mi PC\Proyects\Fede4\comarb-analytics-580ca8f5412c.json",
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

req = RunReportRequest(
    property=f"properties/{PROPERTY_ID}",
    dimensions=[Dimension(name="customEvent:js_ga_sesion_id")],
    metrics=[Metric(name="eventCount")],
    date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
    dimension_filter=filt,
    limit=10000,
)
resp = client.run_report(req)

print(f"Período: {START_DATE} → {END_DATE}")
print(f"Custom dim: customEvent:js_ga_sesion_id\n")

buckets = defaultdict(int)
total = 0
sid_validos = 0
sid_unicos_validos = set()
for row in resp.rows:
    raw = row.dimension_values[0].value
    n = int(row.metric_values[0].value)
    total += n
    if raw == "":
        buckets["(string vacío '')"] += n
    elif raw == "(not set)":
        buckets["(not set)"] += n
    elif raw is None:
        buckets["(None / null)"] += n
    else:
        sid_validos += n
        sid_unicos_validos.add(raw)
        buckets["[valor real SES-*]"] += n

print(f"  {'Bucket':<32}{'Eventos':>10}{'%':>8}")
print(f"  {'-'*32}{'-'*10}{'-'*8}")
for k in sorted(buckets.keys()):
    pct = round(100 * buckets[k] / total, 1) if total else 0
    print(f"  {k:<32}{buckets[k]:>10}{pct:>7}%")
print(f"  {'-'*32}{'-'*10}{'-'*8}")
print(f"  {'TOTAL':<32}{total:>10}")

print(f"\n  Valores reales únicos (sid válidos): {len(sid_unicos_validos)}")
muestra = sorted(sid_unicos_validos)[:5]
for s in muestra:
    print(f"    · {s}")
