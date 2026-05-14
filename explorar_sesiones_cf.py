"""Exploración: encontrar ejemplos concretos del problema CF/SID post-fix.
Cruza CF con SID por (cuit, fecha) para ver qué tan grave es la mezcla."""
import io
import sys
import pandas as pd

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

df = pd.read_csv("ps_flujo_sesiones.csv", dtype={"cuit": str})
df.columns = [c.lstrip("﻿") for c in df.columns]
df["cuit"] = df["cuit"].fillna("").str.replace(".0", "", regex=False)
df["fecha"] = df["fecha"].astype(str)

post = df[df["fecha"] >= "2026-04-22"].copy()
post["secuencia"] = post["secuencia_str"].fillna("").astype(str)

# El separador real es " → " (con espacios)
def split_seq(s: str) -> list[str]:
    if not s:
        return []
    if " → " in s:
        return s.split(" → ")
    if "→" in s:
        return s.split("→")
    return s.split("|")

post["seq_list"] = post["secuencia"].apply(split_seq)
post["n_continuar_0"] = post["seq_list"].apply(lambda L: sum(1 for x in L if x.strip() == "PS_boton_continuar_0"))
post["n_presentar"] = post["seq_list"].apply(
    lambda L: sum(1 for x in L if x.strip() in
        ("PS_boton_presentar_y_salir", "PS_boton_presentar_y_generar_pago",
         "PS_boton_guardar_borrador_y_salir"))
)

print(f"Post-fix (>= 2026-04-22): {len(post)} sesiones totales")
print(f"  · SID:    {(post['session_key_type']=='SID').sum()} ({round(100*(post['session_key_type']=='SID').sum()/len(post),1)}%)")
print(f"  · CF:     {(post['session_key_type']=='CF').sum()} ({round(100*(post['session_key_type']=='CF').sum()/len(post),1)}%)")
print(f"  · SINGLE: {(post['session_key_type']=='SINGLE').sum()}")

cf  = post[post["session_key_type"] == "CF"].copy()
sid = post[post["session_key_type"] == "SID"].copy()


def imprimir_caso(row, etiqueta: str):
    print(f"\n  ── {etiqueta} ──")
    sid_label = f"SES={row['session_id']}" if pd.notna(row['session_id']) and str(row['session_id']) not in ("", "nan") else "(sin sid)"
    print(f"  CUIT={row['cuit']}  fecha={row['fecha']}  tipo={row['session_key_type']}  {sid_label}")
    print(f"  primer_ts={row['primer_ts']}  ultimo_ts={row['ultimo_ts']}  dur={row['duracion_seg']}s  n_eventos={row['n_eventos']}")
    print(f"  paso_max={row['paso_max_alcanzado']}  estado={row['estado_final']}  n_volver={row['n_volver']}  errores={row['n_errores']}")
    if pd.notna(row['escape_event']) and str(row['escape_event']) not in ("", "nan"):
        print(f"  escape={row['escape_event']} (paso {row['escape_paso']})")
    print(f"  UA: {row['browser']}/{row['operating_system']}/{row['device_category']}")
    print(f"  N continuar_0 = {row['n_continuar_0']}  ·  N presentar = {row['n_presentar']}")
    seq = row["seq_list"]
    print(f"  Secuencia ({len(seq)} eventos):")
    for i, ev in enumerate(seq, 1):
        print(f"    {i:>3}. {ev.strip()}")


# ─────────────────────────────────────────────────────────
# CASO A: CUIT/fecha donde coexisten SID y CF
# (la prueba más fuerte de mezcla potencial: el mismo
# contribuyente generó eventos linkeados a sid + eventos sin sid)
# ─────────────────────────────────────────────────────────
print("\n" + "═"*78)
print("  CASO A: CUIT/fecha con SID + CF coexistiendo")
print("    (eventos del mismo usuario partidos: parte fue al SID, parte cayó a CF)")
print("═"*78)
post_v = post[post["cuit"] != ""].copy()
key_counts = post_v.groupby(["cuit", "fecha"])["session_key_type"].apply(set)
mixed_keys = key_counts[key_counts.apply(lambda s: ("SID" in s and "CF" in s))]
print(f"  → {len(mixed_keys)} (cuit, fecha) tienen al mismo tiempo sesión SID + CF")
if len(mixed_keys) > 0:
    for (cuit, fecha) in list(mixed_keys.index)[:3]:
        sub = post_v[(post_v["cuit"] == cuit) & (post_v["fecha"] == fecha)].sort_values("primer_ts")
        print(f"\n  ──── CUIT {cuit} · {fecha} ({len(sub)} sesiones registradas) ────")
        for _, row in sub.iterrows():
            imprimir_caso(row, f"{row['session_key_type']} · {row['primer_ts']}")

# ─────────────────────────────────────────────────────────
# CASO B: CF con n_eventos elevado (sospecha de varios intentos colapsados)
# ─────────────────────────────────────────────────────────
print("\n" + "═"*78)
print("  CASO B: CF largas — n_eventos ≥ 12 o n_volver ≥ 3 o n_continuar_0 ≥ 2")
print("═"*78)
sospechosas = cf[
    (cf["n_eventos"] >= 12) | (cf["n_volver"] >= 3) | (cf["n_continuar_0"] >= 2)
].sort_values("primer_ts", ascending=False)
print(f"  → {len(sospechosas)} sesiones CF sospechosas")
for _, row in sospechosas.head(3).iterrows():
    imprimir_caso(row, f"CF sospechosa · {row['cuit']}/{row['fecha']}")

# ─────────────────────────────────────────────────────────
# CASO C: contrapunto SID limpio (referencia para comparar)
# ─────────────────────────────────────────────────────────
print("\n" + "═"*78)
print("  CASO C: SID típica completada (referencia limpia)")
print("═"*78)
limpia = sid[(sid["paso_max_alcanzado"] >= 6) & (sid["n_eventos"] <= 12)].sort_values(
    "primer_ts", ascending=False).head(2)
for _, row in limpia.iterrows():
    imprimir_caso(row, f"SID limpia · {row['cuit']}/{row['fecha']}")

# ─────────────────────────────────────────────────────────
# CASO D: distribución resumen
# ─────────────────────────────────────────────────────────
print("\n" + "═"*78)
print("  RESUMEN ESTADÍSTICO post-fix")
print("═"*78)
print(f"  Mediana n_eventos        SID={sid['n_eventos'].median():>5.1f}   CF={cf['n_eventos'].median():>5.1f}")
print(f"  Promedio duración (s)    SID={sid['duracion_seg'].mean():>5.0f}   CF={cf['duracion_seg'].mean():>5.0f}")
print(f"  % completaron (≥6)       SID={100*(sid['paso_max_alcanzado']>=6).mean():>5.1f}   CF={100*(cf['paso_max_alcanzado']>=6).mean():>5.1f}")
print(f"  % con errores            SID={100*sid['tiene_errores'].mean():>5.1f}   CF={100*cf['tiene_errores'].mean():>5.1f}")
print(f"  % con escape             SID={100*sid['escape_event'].notna().mean():>5.1f}   CF={100*cf['escape_event'].notna().mean():>5.1f}")
print(f"  Mediana n_volver         SID={sid['n_volver'].median():>5.1f}   CF={cf['n_volver'].median():>5.1f}")
