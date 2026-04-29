# Context: Tarea 1 — Sistemas Distribuidos 2026-1

## Objetivo General

Construir un sistema distribuido con caché para consultas geoespaciales sobre el dataset **Google Open Buildings**, enfocado en la Región Metropolitana de Santiago. El sistema simula tráfico de empresas de logística urbana y mide el impacto de distintas configuraciones de caché.

---

## Arquitectura del Sistema

Cuatro servicios independientes que se comunican secuencialmente, todos desplegados con **Docker / docker-compose**:

```
Generador de Tráfico → Sistema de Caché (Redis) → Generador de Respuestas
                                ↓
                     Almacenamiento de Métricas
```

### 1. Generador de Tráfico

- Simula solicitudes de empresas de reparto consultando zonas de Santiago.
- Genera consultas sintéticas (sin BD externa) con dos distribuciones de arribo:
  - **Zipf (Ley de Potencia):** algunas zonas/queries son mucho más frecuentes.
  - **Uniforme:** todas las zonas/queries tienen igual probabilidad.
- Cada consulta incluye: tipo (`Q1`–`Q5`), zona geográfica (`Z1`–`Z5`), parámetros asociados.

### 2. Sistema de Caché (Redis)

- Intercepta todas las consultas entrantes.
- **Cache hit:** retorna la respuesta directamente y registra el evento en métricas.
- **Cache miss:** delega al Generador de Respuestas, almacena el resultado con TTL.
- Implementado sobre **Redis** con:
  - TTL configurable por tipo de consulta.
  - Políticas de evicción: `LRU`, `LFU`, `FIFO`.
  - Tamaños de caché a evaluar: `50 MB`, `200 MB`, `500 MB`.
- Cache key format:
  - Q1: `count:{zona_id}:conf={confidence_min}`
  - Q2: `area:{zona_id}:conf={confidence_min}`
  - Q3: `density:{zona_id}:conf={confidence_min}`
  - Q4: `compare:density:{zona_a}:{zona_b}:conf={confidence_min}`
  - Q5: `confidence_dist:{zona_id}:bins={bins}`

### 3. Generador de Respuestas

- Procesa las consultas delegadas por la caché.
- Carga el dataset en memoria al iniciar (DataFrame o estructura equivalente).
- Computa resultados directamente en memoria, simulando latencia de procesamiento geoespacial.
- Devuelve la respuesta al caché y al sistema de métricas.
- **No hay base de datos en tiempo de ejecución.**

### 4. Almacenamiento de Métricas

- Registra todos los eventos del sistema: hits, misses, latencias, throughput, evictions.
- Usado para análisis posterior bajo distintas configuraciones.

---

## Dataset

**Google Open Buildings** — subconjunto de la Región Metropolitana.

Campos precargados en memoria:

| Campo            | Tipo  | Descripción                   |
| ---------------- | ----- | ----------------------------- |
| `latitude`       | FLOAT | Latitud del centroide         |
| `longitude`      | FLOAT | Longitud del centroide        |
| `area_in_meters` | FLOAT | Área aproximada (m²)          |
| `confidence`     | FLOAT | Confianza de detección [0, 1] |

---

## Zonas Geográficas Predefinidas (Bounding Boxes)

| Zona (ID)            | lat_min | lat_max | lon_min | lon_max |
| -------------------- | ------- | ------- | ------- | ------- |
| Providencia (Z1)     | −33.445 | −33.420 | −70.640 | −70.600 |
| Las Condes (Z2)      | −33.420 | −33.390 | −70.600 | −70.550 |
| Maipú (Z3)           | −33.530 | −33.490 | −70.790 | −70.740 |
| Santiago Centro (Z4) | −33.460 | −33.430 | −70.670 | −70.630 |
| Pudahuel (Z5)        | −33.470 | −33.430 | −70.810 | −70.760 |

---

## Tipos de Consultas (Q1–Q5)

### Q1 — Conteo de edificios en una zona

- **Params:** `zone_id`, `confidence_min` (default 0.0)
- **Lógica:** `sum(1 for r in records if r.confidence >= confidence_min)`

### Q2 — Área promedio y total

- **Params:** `zone_id`, `confidence_min`
- **Retorna:** `{avg_area, total_area, n}`

### Q3 — Densidad de edificaciones por km²

- **Params:** `zone_id`, `confidence_min`
- **Lógica:** `q1_count(zone_id) / zone_area_km2[zone_id]`

### Q4 — Comparación de densidad entre dos zonas

- **Params:** `zone_a`, `zone_b`, `confidence_min`
- **Retorna:** `{zone_a: densidad, zone_b: densidad, winner: zona_ganadora}`

### Q5 — Distribución de confianza en una zona

- **Params:** `zone_id`, `bins` (default 5)
- **Retorna:** lista de buckets `[{bucket, min, max, count}]`

---

## Métricas a Recopilar y Analizar

| Métrica            | Definición                             |
| ------------------ | -------------------------------------- |
| Hit rate           | `hits / (hits + misses)`               |
| Throughput         | Consultas exitosas / segundo           |
| Latencia p50 / p95 | Percentiles de tiempo de respuesta     |
| Eviction rate      | Evictions / minuto                     |
| Cache efficiency   | `(hits·t_cache − misses·t_db) / total` |

---

## Análisis Requerido en el Informe

### Impacto de la distribución de tráfico

- Comparar hit rate y miss rate entre distribución Zipf vs. Uniforme.
- Determinar cuál genera mayor estrés/beneficio para la caché y por qué.
- Discutir implicaciones en escenarios reales.

### Efecto de parámetros de la caché

- Comparar al menos **dos políticas de evicción** (LRU, LFU, FIFO).
- Evaluar impacto del **tamaño de caché** (50 MB, 200 MB, 500 MB) en el hit rate.
- Analizar el efecto del **TTL** para cada tipo de consulta (Q1–Q5).

---

## Entregables

| Entregable             | Detalle                                                                              |
| ---------------------- | ------------------------------------------------------------------------------------ |
| Informe técnico        | PDF compilado desde LaTeX con arquitectura, decisiones de diseño y análisis empírico |
| Video de demostración  | ~10 minutos mostrando el sistema en funcionamiento                                   |
| Código fuente          | Repositorio público en GitHub o GitLab (link en Canvas y en el informe)              |
| Archivos de despliegue | `Dockerfile` y/o `docker-compose.yml` + `README.md` con instrucciones claras         |

**Fecha de entrega:** Viernes 17 de abril — vía Canvas  
**Grupos:** hasta 2 integrantes

---

## Stack Tecnológico (sugerido, justificar en informe)

- **Orquestación:** Docker Compose
- **Caché:** Redis (con políticas LRU/LFU/FIFO y TTL)
- **Generador de Tráfico:** Python (numpy para distribuciones Zipf/Uniforme)
- **Generador de Respuestas:** Python + pandas/polars para procesar el dataset en memoria
- **Métricas:** Base de datos relacional en postgresql (debe ser persistente para guardar las metricas)
- **Informe:** LaTeX

---

## Notas Clave de Implementación

- El sistema **no realiza consultas a BD en runtime**: todo el dataset se precarga en memoria al iniciar.
- Las consultas son **completamente sintéticas**: construidas desde zonas predefinidas y parámetros del dataset.
- Cada decisión de diseño debe estar **justificada en el informe** con datos empíricos.
- Redis debe configurarse con `maxmemory` y `maxmemory-policy` para simular los distintos tamaños y políticas.
- El generador de tráfico debe poder configurarse para cambiar entre distribución Zipf y Uniforme sin recompilar.
