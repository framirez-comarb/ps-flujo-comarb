# Scripts efímeros — referencia histórica

Snapshots de un día específico usados durante la validación del fix GTM en
mayo 2026 (ver [SESION_2026-05-06_validacion_gtm.md](../../SESION_2026-05-06_validacion_gtm.md)).

No están pensados para reusarse — la lógica equivalente está cubierta hoy por
`monitor_cobertura_dims.py` y `validacion_post_gtm.py` (ambos con CLI args).

Quedan acá por si se necesita reconstruir el camino de diagnóstico de aquel
fix o ver el formato exacto en que GA4 devolvía las dims durante el rollout.

| Script | Propósito original |
|---|---|
| `_check_sid_value.py` | Verificar si el sid ausente llegaba como `(not set)` o `""` |
| `_check_sid_ayer.py` | Snapshot de cobertura `js_ga_sesion_id` para un solo día |
| `_check_pantalla_ayer.py` | Snapshot de cobertura `pantalla` para un solo día |
