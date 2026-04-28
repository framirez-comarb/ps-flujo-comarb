# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Análisis del flujo de Presentación Simplificada (PS) en SIFERE WEB para COMARB (organismo tributario argentino). Procesa eventos GA4 sesión por sesión para detectar dónde se rompe el flujo: drop-off por paso, errores de validación por campo, escapes a la versión clásica, y caminos de sesiones que no completaron. Genera reporte HTML interactivo + CSVs.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Pipeline completo (queries GA4, requiere credenciales)
python ps_flujo.py -c <service-account>.json --desde 2026-01-01 --hasta 2026-04-27

# Output default → analisis_flujo/ps_flujo.html (con CSVs en el mismo dir)
# Custom output:
python ps_flujo.py -c <creds>.json --desde 2026-01-01 -o ps_flujo.html

# Skip page_views (más rápido, pierde detección de visitas sin click)
python ps_flujo.py -c <creds>.json --skip-pageviews
```

NOTA: `ps_flujo.py` NO tiene flag `--desde-csv`, siempre re-consulta GA4. Para regenerar HTML después de cambios en el `.py`, hay que correr el workflow de GitHub Actions o tener credenciales GA4 a mano. Workaround: editar `ps_flujo.html` manualmente (el JS embebido) y commitear, hasta que el próximo workflow regenere automático.

## Architecture

Single-file script (`ps_flujo.py`) con queries GA4 Data API v1beta sesión por sesión. Steps:

1. **Diagnóstico hostnames** (`extract_hostnames_diagnostic`): lista qué hostnames generaron eventos PS sin filtro, para sanity check
2. **Query 1: eventos PS** (`extract_events`): todos los eventos `PS_*` filtrados por `GA4_HOSTNAME`. Devuelve un row por (cuit, exact_timestamp, eventName, date, ...)
3. **Query 1b: textos de error** (`extract_error_texts`): segunda query separada porque GA4 limita a 9 dimensiones y `texto_del_error` no entraba en la primera. Se mergea por (cuit, ts, date)
4. **Sesiones reconstruidas** (`build_sessions`): agrupa eventos por (cuit, date, ga_session_id) para reconstruir la trayectoria de cada usuario. De ahí salen funnel, drop-off, escapes, etc.
5. **Generación HTML** (`generate_report`): tema dark, KPIs, tabs para cada vista (funnel chart Chart.js, detalle por paso, sesiones que abandonaron, dispositivos/OS/browser, errores por campo con cross-tab, detalle por sesión). CSVs paralelos: `_sesiones`, `_funnel`, `_eventos_raw`

## Key Technical Details

- **GA4 Property ID**: 485388348 ("COMARB - Sifere Web - Presentación Simplificada")
- **GA4 Hostname filter**: `GA4_HOSTNAME` constant, filtra eventos a un hostname específico (no mezclar con tests de otros sitios)
- **Eventos PS tracked**: `PS_paso_X_Y` (paso del funnel), `PS_error_validacion_dj`, `PS_boton_volver`, `PS_boton_escapar_clasica`, `PS_boton_presentar_y_salir`, `PS_boton_presentar_y_generar_pago`, `PS_boton_enviar_encuesta`, etc.
- **Workflows GitHub Actions**: corre lunes a viernes (9:00, 12:00 y 17:00 ART) regenerando todo desde GA4. Para forzar manualmente: `gh workflow run "PS Flujo - Analisis de recorrido GA4" --repo framirez-comarb/ps-flujo-comarb`

## Session Changelog

### Session 2026-04-28 — PDF download rebuild

Mismo rework del flow de descarga PDF que se hizo en `ps-verificacion-comarb` (ver allá para detalle de bug raíz `windowWidth`). Cambios específicos a este repo:

- **Layout PDF**: A4 landscape, container 1000px, margins `[10, 10, 12, 10]` mm. Threshold fit-to-page `PAGE_USABLE_PX=620` con `CARD_GAP_PX=24`
- **Grid horizontal de cards Dispositivo/SO/Browser**: detectado vía heurística `parentDisplay === 'grid'/'flex' + cardSibs.length > 1 + allSameRow`. Cuando se detecta, el grid se fuerza a `gridTemplateColumns: '1fr'` (stack vertical) durante el PDF gen — así cada card usa los 1000px completos y las columnas internas (Dispositivo/Sesiones/% Completaron/% Con Errores/% Llegó a Finalizar DJ) son legibles. Sin esto, las 3 cards en grid de 3 columnas dejaban ~310px por card y los headers se cortaban
- **Compactación CSS de tablas regulares**: `font-size: 0.78rem`, `td/th padding: 0.3rem 0.6rem`, `line-height: 1.35`, `.card { padding: 1rem }`. Reduce ~24% la altura de las tablas pequeñas, permite agrupar más cards por página
- **Wrap natural en headers**: `white-space: normal` SIN `word-break: break-word` (este último partía palabras carácter por carácter en las columnas angostas, "SESIONES" aparecía como `S E S I O N E S` apilado)
- **Lógica fit-to-page**: cards consecutivas se agrupan mientras quepan en una página (similar a ps-verificacion). Por la compactación, el grupo Dispositivo+SO entra junto en una página, y Browser solo en otra. Si la altura de las tablas crece, fit-to-page los separa automáticamente
- **Footer oculto** en el PDF (`.container > footer, body > footer` en hideSelectors)
- **Cross-tab "Campo × Pantalla"** incluida al final del PDF (antes oculta). Cambio: `#tbl-errcampo-cross` removido del array `hideSelectors`, y la lógica de ocultar cards de `#tab-errcampo` cambió de `idx > 0` a `idx >= 2` (sólo oculta la 3ra card "Textos sin clasificar / otros")
- **Layout final del PDF (6 páginas)**:
    - Pág 1: Header + KPIs + Sesiones que alcanzan cada pantalla
    - Pág 2: Detalle por paso
    - Pág 3: Sesiones que abandonaron + Por tipo de dispositivo
    - Pág 4: Por sistema operativo + Por browser
    - Pág 5: Top campos con errores de validación
    - Pág 6: Cross-tab campo × pantalla
- **Diagnóstico vía preview**: `cp ps_flujo.html ../Fede4/_test_ps_flujo.html` para servirlo desde el static-html server de Fede4 (port 8765), después navegar a `http://localhost:8765/_test_ps_flujo.html` y replicar `generarPDF()` en `preview_eval`. Cleanup del archivo `_test_*` al final
- **Sync entre `.py` y `.html`**: como no hay `--desde-csv`, los cambios al `generarPDF()` JS se aplican manualmente al `ps_flujo.html` (parche en lugar a la sección equivalente). El `.py` actualizado igual, así el próximo workflow run regenera HTML correcto. Resolución de conflicts en push: si el workflow ya regeneró HTML antes que pushees, hacer `git pull --rebase` y resolver con `git checkout --ours ps_flujo.html index.html` para mantener tu patch del PDF (los datos se refrescan en el próximo workflow run)
