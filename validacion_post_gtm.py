"""
Validación post-fix GTM.
Chequea si los cambios en tag manager (CUIT en cerrar_encuesta,
exact_timestamp en 22 eventos, Tier A1 ga_session_id, Tier A3 pantalla)
ya propagaron a GA4.
"""
import argparse
import io
import sys
from datetime import date, datetime, timedelta

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

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


def _parse_args():
    today = date.today()
    default_desde = today - timedelta(days=DEFAULT_DAYS)
    p = argparse.ArgumentParser(
        description="Validación post-fix GTM (CHECKs 1-7).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--desde", default=str(default_desde),
                   help="Fecha inicio del período (YYYY-MM-DD).")
    p.add_argument("--hasta", default=str(today),
                   help="Fecha fin del período (YYYY-MM-DD).")
    p.add_argument("-c", "--creds", default=DEFAULT_CREDS,
                   help="Path al JSON de service account de GA4.")
    args = p.parse_args()
    try:
        d1 = datetime.strptime(args.desde, "%Y-%m-%d").date()
        d2 = datetime.strptime(args.hasta, "%Y-%m-%d").date()
    except ValueError as e:
        p.error(f"Formato de fecha inválido: {e}")
    if d1 > d2:
        p.error(f"--desde ({args.desde}) no puede ser posterior a --hasta ({args.hasta})")
    return args


ARGS = _parse_args()
START_DATE = ARGS.desde
END_DATE = ARGS.hasta

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

creds = Credentials.from_service_account_file(ARGS.creds, scopes=SCOPES)
client = BetaAnalyticsDataClient(credentials=creds)


def hostname_filter():
    return FilterExpression(
        filter=Filter(
            field_name="hostName",
            string_filter=Filter.StringFilter(
                value=HOSTNAME,
                match_type=Filter.StringFilter.MatchType.EXACT,
            ),
        )
    )


def event_in_filter(events):
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


def run(dims, metrics, filt, label, limit=10000):
    req = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        dimensions=[Dimension(name=d) for d in dims],
        metrics=[Metric(name=m) for m in metrics],
        date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
        dimension_filter=filt,
        limit=limit,
    )
    try:
        resp = client.run_report(req)
        return resp.rows, None
    except Exception as e:
        return None, str(e)


print("═" * 78)
print(f"  VALIDACIÓN POST-GTM · período {START_DATE} → {END_DATE}")
print("═" * 78)

# ───────────────────────────────────────────────────────────────
# CHECK 1: PS_cerrar_encuesta ahora tiene CUIT
# ───────────────────────────────────────────────────────────────
print("\n▶ CHECK 1: ¿PS_cerrar_encuesta ahora tiene CUIT?")
filt = FilterExpression(
    and_group=FilterExpressionList(expressions=[
        hostname_filter(),
        FilterExpression(filter=Filter(
            field_name="eventName",
            string_filter=Filter.StringFilter(
                value="PS_cerrar_encuesta",
                match_type=Filter.StringFilter.MatchType.EXACT,
            ),
        )),
    ])
)
rows, err = run(["customEvent:CUIT"], ["eventCount"], filt, "cerrar_encuesta")
if err:
    print(f"  ERROR: {err[:200]}")
else:
    total = 0
    con_cuit = 0
    for row in rows:
        c = row.dimension_values[0].value
        n = int(row.metric_values[0].value)
        total += n
        if c and c not in ("(not set)", ""):
            con_cuit += n
    sin_cuit = total - con_cuit
    pct = round(100 * con_cuit / total, 1) if total else 0
    print(f"  Total PS_cerrar_encuesta en período:  {total}")
    print(f"  Con CUIT:     {con_cuit} ({pct}%)")
    print(f"  Sin CUIT:     {sin_cuit}")
    if sin_cuit == 0 and total > 0:
        print(f"  ✓ FIX OK: todos los eventos post-fix tienen CUIT")
    elif total == 0:
        print(f"  ⚠  Sin eventos en el período — esperar más tiempo")
    else:
        print(f"  ✗ Aún hay {sin_cuit} eventos sin CUIT")

# ───────────────────────────────────────────────────────────────
# CHECK 2: exact_timestamp en todos los eventos PS
# ───────────────────────────────────────────────────────────────
print("\n▶ CHECK 2: ¿exact_timestamp ahora viene en todos los eventos PS?")
filt = FilterExpression(
    and_group=FilterExpressionList(expressions=[
        hostname_filter(),
        event_in_filter(EVENTOS_PS),
    ])
)
rows, err = run(
    ["eventName", "customEvent:exact_timestamp"],
    ["eventCount"], filt, "eventos",
)
if err:
    print(f"  ERROR: {err[:200]}")
else:
    from collections import defaultdict
    stats = defaultdict(lambda: {"valido": 0, "sin_ts": 0})
    for row in rows:
        ev = row.dimension_values[0].value
        ts = row.dimension_values[1].value
        n = int(row.metric_values[0].value)
        if ts and ts not in ("(not set)", ""):
            stats[ev]["valido"] += n
        else:
            stats[ev]["sin_ts"] += n
    print(f"  {'Evento':<50}{'Total':>8}{'Válido':>10}{'Sin ts':>10}{'%OK':>8}")
    print(f"  {'-'*50}{'-'*8}{'-'*10}{'-'*10}{'-'*8}")
    for ev in sorted(stats.keys()):
        s = stats[ev]
        tot = s["valido"] + s["sin_ts"]
        pct = round(100 * s["valido"] / tot, 1) if tot else 0
        marca = "✓" if pct >= 99 else ("⚠" if pct >= 50 else "✗")
        print(f"  {ev:<50}{tot:>8}{s['valido']:>10}{s['sin_ts']:>10}{pct:>6}% {marca}")

# ───────────────────────────────────────────────────────────────
# CHECK 3: Tier A1 — customEvent:js_ga_sesion_id
# (registrado en GA4 admin con ese nombre — ver ps_flujo.py L389)
# ───────────────────────────────────────────────────────────────
print("\n▶ CHECK 3: Tier A1 — ¿customEvent:js_ga_sesion_id expuesto y con valores?")
filt = FilterExpression(
    and_group=FilterExpressionList(expressions=[
        hostname_filter(),
        event_in_filter(EVENTOS_PS),
    ])
)
rows, err = run(
    ["customEvent:js_ga_sesion_id"], ["eventCount"], filt, "js_ga_sesion_id",
)
if err:
    if "not a valid dimension" in err or "InvalidArgument" in err:
        print(f"  ✗ NO DISPONIBLE: custom dimension 'js_ga_sesion_id' no está registrada en GA4")
        print(f"    (si se registró recién, puede tardar hasta 48h en estar queryable)")
    else:
        print(f"  ERROR: {err[:200]}")
else:
    total = 0
    con_valor = 0
    unique_ids = set()
    for row in rows:
        sid = row.dimension_values[0].value
        n = int(row.metric_values[0].value)
        total += n
        if sid and sid not in ("(not set)", ""):
            con_valor += n
            unique_ids.add(sid)
    pct = round(100 * con_valor / total, 1) if total else 0
    print(f"  Total eventos PS en período:  {total}")
    print(f"  Con js_ga_sesion_id válido:   {con_valor} ({pct}%)")
    print(f"  Session IDs únicos:           {len(unique_ids)}")
    if con_valor > 0:
        print(f"  ✓ Tier A1 OK — sesiones reales disponibles")
    else:
        print(f"  ✗ js_ga_sesion_id registrado pero llega '(not set)' — revisar tags")

# ───────────────────────────────────────────────────────────────
# CHECK 4: Tier A3 — customEvent:pantalla
# ───────────────────────────────────────────────────────────────
print("\n▶ CHECK 4: Tier A3 — ¿customEvent:pantalla expuesto y con valores?")
rows, err = run(
    ["eventName", "customEvent:pantalla"], ["eventCount"], filt, "pantalla",
)
if err:
    if "not a valid dimension" in err or "InvalidArgument" in err:
        print(f"  ✗ NO DISPONIBLE: custom dimension 'pantalla' no está registrada en GA4")
    else:
        print(f"  ERROR: {err[:200]}")
else:
    total = 0
    con_valor = 0
    by_evento = {}
    for row in rows:
        ev = row.dimension_values[0].value
        p = row.dimension_values[1].value
        n = int(row.metric_values[0].value)
        total += n
        if p and p not in ("(not set)", ""):
            con_valor += n
            by_evento.setdefault(ev, {})[p] = by_evento.setdefault(ev, {}).get(p, 0) + n
    pct = round(100 * con_valor / total, 1) if total else 0
    print(f"  Total eventos PS en período:  {total}")
    print(f"  Con pantalla válida:          {con_valor} ({pct}%)")
    if by_evento:
        print(f"  Muestra de mapping evento → pantalla (top 10):")
        for ev, ps in list(by_evento.items())[:10]:
            top = sorted(ps.items(), key=lambda x: -x[1])[:3]
            print(f"    {ev:<45} → {top}")
    if con_valor > 0:
        print(f"  ✓ Tier A3 OK")
    else:
        print(f"  ✗ pantalla registrado pero llega '(not set)' — revisar tags")

# ───────────────────────────────────────────────────────────────
# CHECK 5: comparativo antes/después para PS_cerrar_encuesta
# ───────────────────────────────────────────────────────────────
print("\n▶ CHECK 5: comparativo histórico para PS_cerrar_encuesta")
print("  (muestra cuántos cerrar_encuesta hay con y sin CUIT por día)")
filt = FilterExpression(
    and_group=FilterExpressionList(expressions=[
        hostname_filter(),
        FilterExpression(filter=Filter(
            field_name="eventName",
            string_filter=Filter.StringFilter(
                value="PS_cerrar_encuesta",
                match_type=Filter.StringFilter.MatchType.EXACT,
            ),
        )),
    ])
)
req = RunReportRequest(
    property=f"properties/{PROPERTY_ID}",
    dimensions=[Dimension(name="date"), Dimension(name="customEvent:CUIT")],
    metrics=[Metric(name="eventCount")],
    date_ranges=[DateRange(start_date="2026-04-10", end_date=END_DATE)],
    dimension_filter=filt,
    limit=10000,
)
try:
    resp = client.run_report(req)
    by_day = {}
    for row in resp.rows:
        d = row.dimension_values[0].value
        c = row.dimension_values[1].value
        n = int(row.metric_values[0].value)
        if d not in by_day:
            by_day[d] = {"con": 0, "sin": 0}
        if c and c not in ("(not set)", ""):
            by_day[d]["con"] += n
        else:
            by_day[d]["sin"] += n
    print(f"  {'Fecha':<12}{'Con CUIT':>10}{'Sin CUIT':>10}{'%OK':>8}")
    print(f"  {'-'*12}{'-'*10}{'-'*10}{'-'*8}")
    for d in sorted(by_day.keys()):
        s = by_day[d]
        tot = s["con"] + s["sin"]
        pct = round(100 * s["con"] / tot, 1) if tot else 0
        fecha_fmt = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        marca = "✓" if pct >= 99 else ("⚠" if pct >= 50 else "✗")
        print(f"  {fecha_fmt:<12}{s['con']:>10}{s['sin']:>10}{pct:>6}% {marca}")
except Exception as e:
    print(f"  ERROR: {str(e)[:200]}")

# ───────────────────────────────────────────────────────────────
# CHECK 6: últimos 20 eventos PS por timestamp con flags Tier A1/A3
# ───────────────────────────────────────────────────────────────
print("\n▶ CHECK 6: últimos 20 eventos PS (orden desc por exact_timestamp)")
filt = FilterExpression(
    and_group=FilterExpressionList(expressions=[
        hostname_filter(),
        event_in_filter(EVENTOS_PS),
    ])
)
req = RunReportRequest(
    property=f"properties/{PROPERTY_ID}",
    dimensions=[
        Dimension(name="eventName"),
        Dimension(name="customEvent:exact_timestamp"),
        Dimension(name="customEvent:js_ga_sesion_id"),
        Dimension(name="customEvent:pantalla"),
    ],
    metrics=[Metric(name="eventCount")],
    date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
    dimension_filter=filt,
    limit=10000,
)
try:
    resp = client.run_report(req)
    eventos = []
    for row in resp.rows:
        ev = row.dimension_values[0].value
        ts = row.dimension_values[1].value
        sid = row.dimension_values[2].value
        pant = row.dimension_values[3].value
        # filtrar eventos sin timestamp para el ranking
        if not ts or ts in ("(not set)", ""):
            continue
        eventos.append({
            "ts": ts,
            "evento": ev,
            "sid": sid if sid and sid not in ("(not set)", "") else "",
            "pant": pant if pant and pant not in ("(not set)", "") else "",
        })
    eventos.sort(key=lambda r: r["ts"], reverse=True)
    top = eventos[:20]
    print(f"  Mostrando top 20 de {len(eventos)} eventos con timestamp en período")
    print(f"  {'#':>3}  {'Timestamp':<20}  {'Evento':<46}  {'sid':^4}  {'pant':^5}")
    print(f"  {'-'*3}  {'-'*20}  {'-'*46}  {'-'*4}  {'-'*5}")
    for i, r in enumerate(top, 1):
        sid_flag = "✓" if r["sid"] else "✗"
        pant_flag = "✓" if r["pant"] else "✗"
        print(f"  {i:>3}  {r['ts']:<20}  {r['evento']:<46}  {sid_flag:^4}  {pant_flag:^5}")
except Exception as e:
    print(f"  ERROR: {str(e)[:200]}")

# ───────────────────────────────────────────────────────────────
# CHECK 7: cross-tab js_ga_sesion_id (sí/no) × device/OS/browser
# Para identificar si la pérdida del ~50% correlaciona con un UA
# ───────────────────────────────────────────────────────────────
print("\n▶ CHECK 7: ¿js_ga_sesion_id ausente correlaciona con device/OS/browser?")
filt = FilterExpression(
    and_group=FilterExpressionList(expressions=[
        hostname_filter(),
        event_in_filter(EVENTOS_PS),
    ])
)
req = RunReportRequest(
    property=f"properties/{PROPERTY_ID}",
    dimensions=[
        Dimension(name="customEvent:js_ga_sesion_id"),
        Dimension(name="deviceCategory"),
        Dimension(name="operatingSystem"),
        Dimension(name="browser"),
    ],
    metrics=[Metric(name="eventCount")],
    date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
    dimension_filter=filt,
    limit=10000,
)

def _imprimir_breakdown(stats: dict, titulo: str):
    print(f"\n  ── {titulo} ──")
    print(f"  {'Categoría':<28}{'Total':>8}{'Con sid':>10}{'Sin sid':>10}{'%OK':>8}")
    print(f"  {'-'*28}{'-'*8}{'-'*10}{'-'*10}{'-'*8}")
    for k in sorted(stats.keys(), key=lambda x: -(stats[x]["con"] + stats[x]["sin"])):
        s = stats[k]
        tot = s["con"] + s["sin"]
        pct = round(100 * s["con"] / tot, 1) if tot else 0
        marca = "✓" if pct >= 99 else ("⚠" if pct >= 50 else "✗")
        print(f"  {k:<28}{tot:>8}{s['con']:>10}{s['sin']:>10}{pct:>6}% {marca}")

try:
    resp = client.run_report(req)
    by_device = {}
    by_os = {}
    by_browser = {}
    by_combo = {}
    for row in resp.rows:
        sid = row.dimension_values[0].value
        dev = row.dimension_values[1].value or "(unknown)"
        os_ = row.dimension_values[2].value or "(unknown)"
        br  = row.dimension_values[3].value or "(unknown)"
        n = int(row.metric_values[0].value)
        tiene_sid = bool(sid and sid not in ("(not set)", ""))
        for stats, key in [
            (by_device, dev),
            (by_os, os_),
            (by_browser, br),
            (by_combo, f"{dev}/{os_}/{br}"),
        ]:
            stats.setdefault(key, {"con": 0, "sin": 0})
            stats[key]["con" if tiene_sid else "sin"] += n
    _imprimir_breakdown(by_device, "Por dispositivo")
    _imprimir_breakdown(by_os, "Por sistema operativo")
    _imprimir_breakdown(by_browser, "Por browser")
    print("\n  ── Top 10 combos device/OS/browser por volumen ──")
    print(f"  {'Combo':<46}{'Total':>8}{'Con sid':>10}{'Sin sid':>10}{'%OK':>8}")
    print(f"  {'-'*46}{'-'*8}{'-'*10}{'-'*10}{'-'*8}")
    combos_ordenados = sorted(
        by_combo.items(), key=lambda kv: -(kv[1]["con"] + kv[1]["sin"]),
    )[:10]
    for k, s in combos_ordenados:
        tot = s["con"] + s["sin"]
        pct = round(100 * s["con"] / tot, 1) if tot else 0
        marca = "✓" if pct >= 99 else ("⚠" if pct >= 50 else "✗")
        print(f"  {k:<46}{tot:>8}{s['con']:>10}{s['sin']:>10}{pct:>6}% {marca}")
except Exception as e:
    print(f"  ERROR: {str(e)[:200]}")

print("\n" + "═" * 78)
print("  FIN VALIDACIÓN")
print("═" * 78 + "\n")
