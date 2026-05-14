"""Cuántos eventos PS del día de ayer (2026-05-04) llegan sin js_ga_sesion_id.
Versión simétrica al check de pantalla, para llevar ambos números a la
conversación de mañana con el dev de GTM."""
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
DATE = "2026-05-04"  # ayer

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
    dimensions=[
        Dimension(name="eventName"),
        Dimension(name="customEvent:js_ga_sesion_id"),
    ],
    metrics=[Metric(name="eventCount")],
    date_ranges=[DateRange(start_date=DATE, end_date=DATE)],
    dimension_filter=filt,
    limit=10000,
)
resp = client.run_report(req)

print(f"Período: solo {DATE} (ayer)")
print(f"Custom dim: customEvent:js_ga_sesion_id\n")

stats = defaultdict(lambda: {"con": 0, "sin": 0, "sids": set()})
total_con = 0
total_sin = 0
sids_unicos_global = set()
for row in resp.rows:
    ev = row.dimension_values[0].value
    sid = row.dimension_values[1].value
    n = int(row.metric_values[0].value)
    if sid and sid not in ("(not set)", ""):
        stats[ev]["con"] += n
        stats[ev]["sids"].add(sid)
        sids_unicos_global.add(sid)
        total_con += n
    else:
        stats[ev]["sin"] += n
        total_sin += n

total = total_con + total_sin
pct_global = round(100 * total_con / total, 1) if total else 0
print(f"  ─── RESUMEN GLOBAL ───")
print(f"  Total eventos PS:     {total}")
print(f"  Con sid:              {total_con} ({pct_global}%)")
print(f"  Sin sid:              {total_sin} ({round(100-pct_global,1)}%)")
print(f"  Session IDs únicos:   {len(sids_unicos_global)}")

print(f"\n  ─── Por evento ───")
print(f"  {'Evento':<48}{'Total':>8}{'Con':>8}{'Sin':>8}{'%OK':>8}")
print(f"  {'-'*48}{'-'*8}{'-'*8}{'-'*8}{'-'*8}")
for ev in sorted(stats.keys()):
    s = stats[ev]
    tot = s["con"] + s["sin"]
    pct = round(100 * s["con"] / tot, 1) if tot else 0
    marca = "✓" if pct >= 99 else ("⚠" if pct >= 50 else "✗")
    print(f"  {ev:<48}{tot:>8}{s['con']:>8}{s['sin']:>8}{pct:>6}% {marca}")

# Sids únicos vistos ayer (muestra)
if sids_unicos_global:
    muestra = sorted(sids_unicos_global)
    print(f"\n  Session IDs vistos ayer ({len(muestra)} únicos):")
    for s in muestra[:20]:
        print(f"    · {s}")
    if len(muestra) > 20:
        print(f"    · ... ({len(muestra)-20} más)")
