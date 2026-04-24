# PS Flujo — Análisis de recorrido de Presentación Simplificada

Pipeline en Python que analiza el recorrido de los usuarios a través del flujo
de Presentación Simplificada de COMARB usando datos de Google Analytics 4.

Genera un reporte HTML interactivo con:

- **Funnel por pantalla** (8 pasos: Datos de jurisdicciones → Generar Pago) con
  drop-off entre pasos, errores, "volver" y escapes.
- **Caminos de abandono** — top 20 secuencias de eventos que NO completaron.
- **Escapes a versión clásica** — desglose por pantalla de origen.
- **Segmentación por dispositivo** — desktop / mobile / tablet, OS, browser.
- **Errores por campo** — clasificador automático del texto del error a
  códigos estables (`impuesto_determinado.total_distribuido`,
  `deducciones.carga_automatica`, etc.) con cross-tab campo × pantalla.
- **Tabla de sesiones** filtrable y ordenable.
- **Filtro de período** dinámico — re-calcula KPIs, funnel y demás vistas
  desde el browser sin re-ejecutar el script.
- **Tema claro / oscuro** con preferencia persistida en localStorage.

## Repositorio relacionado

Funciona en paralelo a [`ps-verificacion-comarb`](https://github.com/framirez-comarb/ps-verificacion-comarb)
(cruce GA4 + DGR Gestión). Usa la misma propiedad GA4 y el mismo service
account.

## Ejecución local

```bash
# Instalar dependencias
pip install -r requirements.txt

# Correr con período por defecto (2026-01-01 → hoy)
python ps_flujo.py -c sa_credentials.json

# Período custom
python ps_flujo.py -c sa_credentials.json --desde 2026-04-01 --hasta 2026-04-30

# Saltar query de page_views (más rápido, pierde detección de visitas sin click)
python ps_flujo.py -c sa_credentials.json --skip-pageviews
```

## Salidas

- `ps_flujo.html` — reporte interactivo (también copiado a `index.html` para
  GitHub Pages).
- `ps_flujo_sesiones.csv` — una fila por sesión reconstruida con todas las
  columnas (engagement, device, errores clasificados, secuencia, etc.).
- `ps_flujo_funnel.csv` — conteos por paso (debug / análisis ad-hoc).
- `ps_flujo_eventos_raw.csv` — eventos crudos de GA4 (debug).

## Lógica de agrupación de sesiones

Estrategia con prioridad de tres niveles (ver `build_sessions` en
`ps_flujo.py`):

1. **`js_ga_sesion_id`** — parámetro custom enviado por GTM desde abril 2026.
   Es la clave de sesión real, independiente de CUIT y fecha. Permite agrupar
   correctamente eventos sin CUIT con su sesión real.
2. **`(CUIT, fecha)`** — fallback histórico para datos previos al despliegue
   del session_id. Mismo criterio que usa `ps_verificacion.py` para
   deduplicar.
3. **Singleton** — eventos sin session_id ni CUIT (ruido residual). Cada uno
   se trata como incidente aislado.

## Automatización

GitHub Actions corre el script de lunes a viernes a las 9:00, 12:00 y 17:00
hora Argentina (UTC-3) y commitea los outputs actualizados al repo. Mismo
schedule que `ps-verificacion-comarb`. Ver `.github/workflows/ps_flujo.yml`.

## Service account

Las credenciales de GA4 viven en el secret `SERVICE_ACCOUNT_JSON` del repo
(GitHub Settings → Secrets and variables → Actions). Localmente, tener el
JSON en la raíz como `sa_credentials.json` (ya está en `.gitignore`).

## Property GA4

Property ID: `485388348` (COMARB - Sifere Web - Presentación Simplificada).
Filtro de hostname: `servicios.comarb.gob.ar` (excluye `localhost` y
`serviciosqa.comarb.gob.ar`).
