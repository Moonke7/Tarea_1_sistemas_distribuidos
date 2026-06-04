Para esta tarea se buscaba evolucionar un sistema de consultas agregando Apache Kafka como sistema de colas de mensajes y desacoplamiento entre servicios. El sistema original utilizaba 4 servicios principales con comunicación síncrona: un generador de tráfico, un sistema de caché, un generador de respuestas y un sistema de almacenamiento de métricas.

En esta segunda entrega, el sistema incorpora Apache Kafka para procesamiento asíncrono, mecanismos de reintentos, Dead Letter Queue (DLQ) y escalamiento horizontal mediante múltiples consumidores.

Para generar las respuestas a las consultas simuladas, se utiliza el dataset [Google Open Buildings](https://sites.research.google/gr/open-buildings/).

# Tecnologías utilizadas

Para este proyecto se utilizó:

- Docker : Utilizado para generar contenedores y darle portabilidad al proyecto.
- Python : Utilizado para toda la lógica del sistema, incluyendo el uso de la biblioteca FastAPI para generar las conexiones entre contenedores.
- Postgres : Utilizado como base de datos SQL relacional para guardar las métricas del sistema.
- Redis : Utilizado para el sistema de caché.
- Apache Kafka : Utilizado como sistema de mensajería para colas asíncronas entre el generador de tráfico y los consumidores.

# Ejecución

Inicialmente, se debe clonar este repositorio y guardarlo dentro del directorio de su preferencia.

Luego, dentro del proyecto, debe dirigirse al directorio `scripts`, dentro de este encontrará archivos `.sh`, los cuales automatizan procesos como:

> [!IMPORTANT]
> Estos scripts están pensados para ser utilizados en sistemas Unix (Linux-macOS)

## Iniciar la simulación

Para ejecutar una unica ejecucion y ver los logs, se utiliza:

```bash
./restart_and_logs.sh
```

Para levantar los contenedores del Docker en todos los escenarios solicitados para el sistema de forma automática se utiliza el script `run_scenarios.sh`

```bash
./run_scenarios.sh
```

Este script ejecuta automáticamente múltiples escenarios:

### Escenarios normales:

- **1 consumidor**: Procesamiento con un solo consumidor Kafka
- **5 consumidores**: Escalamiento horizontal con 5 consumidores
- **10 consumidores**: Escalamiento horizontal con 10 consumidores

### Escenarios de falla:

- **Falla temporal**: Simula caída del generador de respuestas durante 15 segundos
- **Spike de tráfico**: Incremento repentino de la tasa de consultas (50 qps → 1000 qps)

Cada escenario genera un archivo JSON en `resulta2/` con métricas comparativas.

## Acceder a la base de datos

Para poder acceder a la base de datos y visualizar las estadísticas de las ejecuciones se utiliza el script `stats_db.sh`

```bash
./stats_db.sh
```

Al ejecutarlo, se mostrará en la terminal:

**Métricas de caché:**

- Hit rate por zona y total
- Keys expulsadas por TTL y política de remoción

**Métricas de Kafka:**

- Success rate (consultas exitosas vs fallidas)
- Throughput (consultas/segundo)
- Latencia p50/p95
- Retry rate y recovery rate
- DLQ rate
- Backlog size (peak lag)
- Recovery time

# Visualizar resultados de las ejecuciones

Al ejecutar el sistema, para cada iteración de consultas se generará un nuevo archivo `json` en la carpeta `resulta2` (la cual se encuentra en la raíz del proyecto y se crea automaticamente).
Con estos archivos es posible comparar las estadísticas de las diferentes configuraciones posibles de la simulación.

# Funcionamiento

Esta implementación utiliza:

- Bounding boxes para limitar el conjunto de datos en el que buscar
- 7 contenedores Docker: kafka, trafic, redis, cache, responses, consumer, monitor, metrics

### Bounding boxes

Para limitar las zonas a las que se realizarán consultas, se utilizan las siguientes bounding boxes:

| Zona (ID)            | lat_min | lat_max | lon_min | lon_max |
| :------------------- | :-----: | :-----: | :-----: | :-----: |
| Providencia (Z1)     | -33.445 | -33.420 | -70.640 | -70.600 |
| Las Condes (Z2)      | -33.420 | -33.390 | -70.600 | -70.550 |
| Maipú (Z3)           | -33.530 | -33.490 | -70.790 | -70.740 |
| Santiago Centro (Z4) | -33.460 | -33.430 | -70.670 | -70.630 |
| Pudahuel (Z5)        | -33.470 | -33.430 | -70.810 | -70.760 |

Estos permiten simplificar las consultas, facilitando de esta manera el guardarlas en caché.

## Flujo del sistema

### Generador de trafico

El generador de trafico simula ser una organización de logística generando consultas predeterminadas ciertas zonas.
El trafico de estas consultas por zona esta definido por 2 distribuciones:

- Power-law
- Uniforme

#### Power-law

Para este proyecto, esta distribución fue simulada condicionando la probabilidad de generación de consultas para cada zona. Los porcentajes utilizados fueron los siguientes:

| Zona (ID) | Sector          | Probabilidad |
| :-------: | :-------------- | :----------: |
|  **Z1**   | Providencia     |  0.43 (43%)  |
|  **Z2**   | Las Condes      |  0.25 (25%)  |
|  **Z3**   | Maipú           |  0.16 (16%)  |
|  **Z4**   | Santiago Centro |  0.10 (10%)  |
|  **Z5**   | Pudahuel        |  0.06 (6%)   |

#### Uniforme

Para simular esta distribución, se utiliza el modulo `random` de Python, el cual seleccionará una zona aleatoria para cada consulta.

## Arquitectura con Apache Kafka

En la Tarea 2, el sistema evoluciona de comunicación síncrona a asíncrona usando Apache Kafka:

### Componentes adicionales:

- **Apache Kafka**: Broker de mensajería que actúa como intermediario
- **Consumidores Kafka**: Procesan mensajes de las colas, consultan caché y derivan a respuestas si es necesario
- **Monitor de backlog**: Mide el tamaño de las colas en tiempo real
- **Tópicos de reintento**: Reciben consultas fallidas para reintentarlas
- **Dead Letter Queue (DLQ)**: Almacena consultas que fallaron después del máximo de reintentos

### Flujo del sistema:

1. El generador de tráfico publica consultas en el tópico `geo-queries` de Kafka
2. Los consumidores Kafka obtienen mensajes y consultan el sistema de caché
3. Si hay cache hit, responden inmediatamente
4. Si hay cache miss, derivan al generador de respuestas
5. Si falla el procesamiento, el mensaje se reenvía al tópico `geo-retry`
6. Después de 3 reintentos fallidos, el mensaje va a la DLQ (`geo-dlq`)

### Sistema de Caché

Todas las consultas creadas por el generador de trafico son recibidas por el sistema de caché, el cual está implementado con `redis`, este guarda `keys` de la forma:

```
tipo:zona:parametros
```

Si la consulta esta cacheada, este responde al generador de trafico con la respuesta y a la vez registra el `caché hit` en las métricas.

Por otro lado, si no está cacheada, deriva la consulta al Generador de respuestas

### Generador de respuestas

El generador de respuestas carga el dataset a memoria inmediatamente tras levantarse mediante la biblioteca `Pandas`.
Luego, con este cargado, cada vez que recibe consultas desde el caché, se encarga de responderlas utilizando las funciones pre hechas (una para cada tipo de consulta).

Una vez obtenidas las respuestas de las consultas, estas son devueltas al sistema de caché, el cual guarda la respuesta en el caché y registra el `caché miss` en las metricas.

### Métricas

El sistema registra métricas en Postgres y las exporta a archivos JSON:

#### Métricas de caché (Tarea 1):

- query_type, zone_id, cache_key, source, latency_ms
- Hit rate por zona y total
- Eviction rate (keys expulsadas por TTL o política)

#### Métricas de Kafka (Tarea 2):

- **Throughput**: Consultas procesadas exitosamente por segundo
- **Latencia p50/p95**: Percentiles de tiempo de respuesta (desde creación del mensaje hasta procesamiento)
- **Retry rate**: Porcentaje de consultas reenviadas a tópicos de reintento
- **Recovery rate**: Porcentaje de consultas recuperadas exitosamente tras fallos
- **DLQ rate**: Porcentaje de consultas enviadas a la Dead Letter Queue
- **Backlog size**: Cantidad de mensajes pendientes en las colas Kafka
- **Recovery time**: Tiempo necesario para vaciar la cola tras una falla
- **Success rate**: Porcentaje de consultas exitosas vs fallidas
