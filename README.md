
Para esta tarea se buscaba crear un sistema de optimización de consultas geoespaciales usando 4 servicios principales; Un generador de tráfico que creará todas las consultas simulando empresas de logística , un sistema de caché que interceptará las peticiones para devolver la respuesta si ya está guardada , un generador de respuestas que procesará los datos en memoria si la caché falla y un sistema de almacenamiento de métricas, el cual registrará todos los eventos y rendimiento generados dentro del sistema.

Para generar las respuestas a las consultas simuladas, se utiliza el dataset [Google Open Buildings](https://sites.research.google/gr/open-buildings/).

# Tecnologías utilizadas

Para este proyecto se utilizó:

- Docker : Utilizado para generar contenedores y darle portabilidad al proyecto.
- Python : Utilizado para toda la lógica del sistema, incluyendo el uso de la biblioteca FastAPI para generar las conexiones entre contenedores.
- Postgres : Utilizado como base de datos SQL relacional para guardar las métricas del sistema.
- Redis : Utilizado para el sistema de caché.
# Ejecución

Inicialmente, se debe clonar este repositorio y guardarlo dentro del directorio de su preferencia.

Luego, dentro del proyecto, debe dirigirse al directorio `scripts`, dentro de este encontrará archivos `.sh`, los cuales automatizan procesos como:


==Estos scripts están pensados para ser utilizados en sistemas Unix (Linux-macOS)==

## Iniciar la simulación 

Para levantar los contenedores del Docker necesarios para el sistema de forma automática se utiliza el script `run_scenarios.sh`

```bash
./run_scenarios.sh
```

Al ejecutar esto, iniciará todos los contenedores, y en cuanto estos estén listos, comenzarán a generar trafico automáticamente.

## Acceder a la base de datos 

Para poder acceder a la base de datos y visualizar las estadísticas de las ejecuciones se utiliza el script `stats_db.sh`

```bash
./stats_db.sh
```

Al ejecutarlo, se mostrará en la terminal: 

- La cantidad de consultas por zona
- La cantidad de consultas totales
- El porcentaje de `hit rate` por zona 
- El porcentaje de `hit rate` total
- La cantidad de `keys` expulsadas del caché por TTL
- La cantidad de `keys` expulsadas del caché por la ``política de remoción``


# Visualizar resultados de las ejecuciones

Al ejecutar el sistema, para cada iteración de consultas se generará un nuevo archivo `json` en la carpeta `results` (la cual se encuentra en la raíz del proyecto). 
Con estos archivos es posible comparar las estadísticas de todas las configuraciones posibles de la simulación.
# Funcionamiento

Esta implementación utiliza:
- Bounding boxes para limitar el conjunto de datos en el que buscar
- 4 contenedores Docker, uno para cada servicio necesario para el sistema.
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

Para simular esta distribución, se utiliza el modulo ``random`` de Python, el cual seleccionará una zona aleatoria para cada consulta.


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

Una vez obtenidas las respuestas de las consultas, estas son devueltas al sistema de caché, el cual guarda la respuesta en el caché y registra el ``caché miss`` en las metricas.


### Metricas

Este contenedor guarda las métricas en una base de datos SQL relacional y solo recibe datos desde el sistema de caché.

Guarda las metricas:
- query_type  
- zone_id
- cache_key
- source  (Caché o Generador de respuestas)
- latency_ms (Tiempo que se tardó en responder al Generador de trafico)
 

