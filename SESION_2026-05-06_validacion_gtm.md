# Sesión 2026-05-06 — Validación de fixes GTM (Tier A1 sid + Tier A3 pantalla)

Documento de continuidad para retomar el trabajo desde otra PC. Resume
toda la sesión del 2026-05-06: hallazgos, decisiones, scripts creados,
estado de cada custom dimension y pendientes.

---

## Contexto inicial

El objetivo era validar si los cambios aplicados en GTM correspondientes
al **Tier 3** habían propagado a GA4. Los cambios cubrían:

1. **CUIT en `PS_cerrar_encuesta`** (antes el evento llegaba sin CUIT)
2. **`exact_timestamp` en los 28 eventos PS** (uniformizar resolución temporal)
3. **Tier A1: custom dim `ga_session_id`** (registrar sesión real en GA4)
4. **Tier A3: custom dim `pantalla`** (mapeo evento → pantalla del flujo)

Script base: [validacion_post_gtm.py](validacion_post_gtm.py) — ya
existía con CHECKs 1–5; se extendió a CHECKs 6–7 durante esta sesión.

---

## Hallazgo crítico: el nombre real es `js_ga_sesion_id`

La custom dimension del Tier A1 **no se llama `ga_session_id`** (que es
como decía el script viejo) sino **`js_ga_sesion_id`** (con typo
histórico: `sesion` con una sola "s"). Está registrada en GA4 admin
desde el 2026-04-22. Confirmado mirando [ps_flujo.py:389](ps_flujo.py:389)
donde el script productivo ya usa el nombre correcto.

**El nombre quedó así por consistencia** — renombrar ahora rompería las
queries existentes y obligaría a esperar 30+ días a que repropague.

---

## Estado final de cada fix

### CHECK 1 — CUIT en `PS_cerrar_encuesta`
**✓ FIX APLICADO** (período 2026-04-21 → 2026-05-04: 6/6 = 100%)

Histórico:
```
2026-04-14 a 17:    0% (todos sin CUIT)
2026-04-20:        33% (transición)
2026-04-21+:      100% sostenido
```

### CHECK 2 — `exact_timestamp` en eventos PS
**✓ FIX APLICADO** (período 2026-04-21 → 2026-05-04: 25/25 eventos al 100%)

### CHECK 3 — Tier A1 `js_ga_sesion_id`
**✓ FIX APLICADO**, estabilizado el 2026-05-06.

Curva de propagación:
```
2026-05-04 (Lu) → 65.2%   (deploy parcial)
2026-05-05 (Ma) → 71.0%   (propagación)
2026-05-06 (Mi) → 100%    (estabilizado, 194 eventos)
```

**Diagnóstico de la causa raíz** (confirmado por el dev de GTM):
el parámetro `js_ga_sesion_id` estaba configurado en la **Etiqueta de
configuración de Google** (Google Tag) en vez de en cada uno de los
**Tags de eventos PS** individualmente. Como consecuencia, cuando un
usuario disparaba un evento PS antes de que la Google Tag terminara
de inicializar la cookie `_ga_*`, todos los eventos de esa sesión
salían con `(not set)`. Esto producía el patrón session-level del 50%
de cobertura: sesiones donde llegaban TODOS los eventos con sid vs
sesiones donde llegaban TODOS sin sid.

**Fix aplicado**: mover el parámetro a cada uno de los 28 Tags de
eventos PS individualmente, evaluando la variable JS dentro del fire
de cada tag en lugar de heredarla del config.

**Forma exacta en que llegaba**: `(not set)`, no string vacío
(verificado con `_check_sid_value.py`). Esto significa que la variable
GTM devolvía `undefined`, no `""`.

### CHECK 4 — Tier A3 `pantalla`
**✓ FIX APLICADO**, estabilizado el 2026-04-29.

Curva de propagación:
```
2026-04-21 a 23:  0%    (pre-fix)
2026-04-24 (Vi):  90.1% (deploy)
2026-04-27 a 28:  80-86% (transición, 4 días por feriado +finde)
2026-04-29 (Mi)+: 100%  (estabilizado)
```

Misma receta que se aplicó después a `js_ga_sesion_id` (mover de
Google Tag a cada Event Tag).

### CHECK 5–7 — adicionales
- **CHECK 5**: comparativo histórico día a día de CUIT en `PS_cerrar_encuesta`
- **CHECK 6** (nuevo): top 20 eventos PS por timestamp con flags sid/pantalla — útil para ver patrón session-level
- **CHECK 7** (nuevo): cross-tab sid × device/OS/browser — confirmó que la pérdida del 50% era uniforme entre browsers (descartó hipótesis "bloqueo de cookies / ITP")

---

## Análisis de impacto en `ps_flujo.py` (lectura SID/CF/SINGLE)

[ps_flujo.py:730](ps_flujo.py:730) `build_sessions()` agrupa eventos con
prioridad jerárquica:

| Tipo | Significa | Cuándo se usa |
|---|---|---|
| **SID** | Session ID real (`SES-YYYYMMDD-HHMMSS-NNNNN`) | Evento llegó con `js_ga_sesion_id` válido |
| **CF** | Cuit + Fecha (heurística histórica) | Evento sin sid pero con CUIT válido |
| **SINGLE** | Singleton huérfano | Evento sin sid ni CUIT |

### Distribución observada en el período crítico (2026-04-22 → 2026-05-04)

70 sesiones post-fix totales:
- **SID**: 40 (57%) — sesiones reales limpias
- **CF**: 30 (43%) — fallback heurístico
- **SINGLE**: 0 (eliminado por fix CUIT en `cerrar_encuesta`)

### Casos concretos encontrados

Cruce CUIT/fecha con SID + CF coexistiendo: **11 ocurrencias**.

**Ejemplo crítico** (CUIT 23175417079, 2026-04-22):
- SID (11:09:13–11:14:39): completó. paso_max=6.
- CF (11:11:12–11:12:33): escapó. paso_max=2.

Los timestamps de la CF están **dentro** del rango de la SID. Es la
misma sesión física partida en dos filas porque el tag GTM falló
intermitentemente. Resultado: el reporte cuenta 1 escape + 1 completado
cuando físicamente fue 1 sesión que completó.

### Impacto cuantificado en KPIs

Diferencia SID vs CF en el cohort post-fix:
- `% completaron`: 52.5% (SID) vs 46.7% (CF) → Δ ~6pp
- `% con escape`: 55% (SID) vs 47% (CF) → Δ ~8pp
- `% con errores`: similar (~12-13%)

**Una vez estabilizado el sid al 100% (a partir del 2026-05-07), la
columna CF queda residual y los KPIs vuelven a ser fiables.**

---

## Recomendación que se le dió al dev de GTM (ya aplicada)

> Mover el parámetro `js_ga_sesion_id` (y `pantalla` antes) de la
> Etiqueta de configuración de Google a cada uno de los 28 Tags de
> eventos PS individualmente. En cada Event Tag, en "Event parameters":
>
> | Parameter Name | Value |
> |---|---|
> | `js_ga_sesion_id` | `{{nombre_de_la_variable_que_lee_session_id}}` |

Esto garantiza que el parámetro se envía en el payload de cada evento,
independientemente de cuándo corrió la Google Tag.

---

## Workflows GitHub Actions

### Fallo del 2026-05-05 (transient, NO bloqueo dgw)

Tanto `ps-flujo-comarb` (run `25381235987`) como `ps-verificacion-comarb`
(run `25379042826`) fallaron el mismo día con el mismo error:

```
The job was not acquired by Runner of type hosted even after multiple attempts
```

**Diagnóstico**: ningún runner hosted disponible en el slot pico de
GitHub Actions. NO fue por el bloqueo dgw / comarb data mencionado por
el equipo. Los runs siguientes en ambos repos corrieron en <2min,
confirmando que la infra está sana.

### Por qué `ps-flujo-comarb` es inmune al bloqueo dgw

El workflow [.github/workflows/ps_flujo.yml](.github/workflows/ps_flujo.yml)
solo consume:
- github.com (checkout, push)
- pypi.org (deps)
- `analyticsdata.googleapis.com` (GA4 Data API directo desde el runner)

Ningún endpoint pasa por la red interna de comarb. `ps-verificacion-comarb`
sí toca DGR pero el run del 2026-05-05 16:20 UTC corrió en 59s OK →
el acceso DGR del service account está sano.

### Cómo distinguir un fallo real de bloqueo vs transient runner

| Síntoma | Causa | Acción |
|---|---|---|
| `Job was not acquired by Runner...` | Transient GitHub | Esperar siguiente slot |
| Error HTTP 401/403 en step `Ejecutar` | Bloqueo real DGR | Verificar marcas de revalidación |
| Timeout en step `Ejecutar` (sin error explícito, >5min) | Bloqueo de red | Verificar IPs de dgw / firewall |

---

## Scripts agregados al repo en esta sesión

| Archivo | Propósito | Conservar |
|---|---|---|
| `validacion_post_gtm.py` | Actualizado: nombre `js_ga_sesion_id`, path absoluto a creds, rango 2026-04-21 → 2026-05-04, CHECKs 6 y 7 agregados | Sí |
| `monitor_cobertura_dims.py` | Cobertura día a día de `pantalla` y `js_ga_sesion_id`. Útil para verificación periódica | Sí |
| `explorar_sesiones_cf.py` | Encuentra ejemplos concretos de sesiones CF post-fix con potencial mezcla. Útil si en el futuro vuelve a haber una dim parcial | Sí |
| `_check_sid_value.py` | Uso único: determinar si el sid ausente llega como `(not set)` o `""`. Conservado por valor de referencia | Efímero (dejarlo) |
| `_check_sid_ayer.py` | Uso único: snapshot de cobertura sid para un solo día | Efímero (dejarlo) |
| `_check_pantalla_ayer.py` | Uso único: snapshot de cobertura pantalla para un solo día | Efímero (dejarlo) |
| `SESION_2026-05-06_validacion_gtm.md` | Este documento | Sí |

Path a las credenciales GA4 (hardcoded en los scripts):
```
G:\Otros ordenadores\Mi PC\Proyects\Fede4\comarb-analytics-580ca8f5412c.json
```

Service account:
```
ga4-reader@comarb-analytics.iam.gserviceaccount.com
```

Property ID GA4: `485388348` (COMARB - Sifere Web - Presentación Simplificada)

---

## Pendientes al cerrar la sesión

### Verificación de estabilización
- [ ] **Jueves 2026-05-07**: correr `python monitor_cobertura_dims.py` y
      verificar que `js_ga_sesion_id` sigue al 100% con muestra
      significativa (>100 eventos).
- [ ] **Viernes 2026-05-08**: misma verificación. Si dos días en fila
      al 100%, fix queda confirmado.

### Confirmación a nivel reporte
- [ ] Una vez que el workflow regenere `ps_flujo_sesiones.csv` con
      datos de 2-3 días con sid al 100%, verificar que el ratio
      SID/CF haya pasado del **57/43 actual** a **>95/<5**. Comando:
      ```python
      df = pd.read_csv("ps_flujo_sesiones.csv")
      post = df[df["fecha"] >= "2026-05-06"]
      print(post["session_key_type"].value_counts(normalize=True))
      ```

### Resolver outlier de `pantalla`
- [ ] Hoy aparecieron 3/194 eventos sin pantalla (1.5%). Si persiste
      varios días, identificar de qué evento/UA viene y reportar al
      dev de GTM. Si es ruido residual (página cacheada), ignorar.

### Mejoras opcionales a `ps_flujo.py` / HTML (baja prioridad)
Con sid al 100%, **estas mejoras pierden urgencia**. Se mantienen como
referencia por si en el futuro vuelve a haber una dim parcial:
- Toggle/dropdown en HTML para filtrar por `session_key_type`
- Doble KPI ("todas las sesiones" vs "solo SID confiables")
- Banner en header con `% sesiones SID` del período

---

## Cómo retomar desde otra PC

1. **Clonar / pull el repo**:
   ```bash
   git clone https://github.com/framirez-comarb/ps-flujo-comarb.git
   cd ps-flujo-comarb
   git fetch
   git checkout claude/nostalgic-pascal-40d0aa  # o el branch actual
   git pull
   ```

2. **Conseguir las credenciales**: el JSON
   `comarb-analytics-580ca8f5412c.json` está fuera del repo
   (`Fede4/`). Sincronizar esa carpeta o copiar el JSON a una ruta
   accesible y **editar el `creds = Credentials.from_service_account_file(...)`**
   en los scripts (`validacion_post_gtm.py`, `monitor_cobertura_dims.py`,
   `explorar_sesiones_cf.py`, `_check_*.py`) para apuntar a la ruta
   correcta en la PC nueva.

3. **Instalar deps**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Re-correr la verificación**:
   ```bash
   python monitor_cobertura_dims.py   # día a día actualizado
   python validacion_post_gtm.py      # CHECKs 1-7 completos
   ```

5. **Si hay nueva versión del HTML** desde el workflow:
   ```bash
   git pull   # los workflows automáticos ya regeneraron los CSVs y HTML
   ```

---

## Referencias cruzadas

- **CLAUDE.md** (raíz del repo) — instrucciones del proyecto y session changelog
- **README.md** — descripción operativa
- **ps_flujo.py** — script productivo (sigue siendo el de referencia)
- **ps_flujo.html / index.html** — reporte generado
- **Workflow**: corre lunes a viernes 9:00 / 12:00 / 17:00 ART
- **Comando para forzar regeneración manual**:
  ```bash
  gh workflow run "PS Flujo - Analisis de recorrido GA4" --repo framirez-comarb/ps-flujo-comarb
  ```
