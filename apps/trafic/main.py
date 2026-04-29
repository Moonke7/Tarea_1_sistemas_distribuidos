import os
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from dist_uniforme import generar_query_uniforme
from dist_zipf import generar_query_zipf

CACHE_URL = os.environ.get("CACHE_URL", "http://cache:5000/query")


def generar_cache_key(query_data):
    q_type = query_data["query"]
    if q_type == "Q1":
        return f"count:{query_data['zone_id']}:conf={query_data['confidence_min']}"
    elif q_type == "Q2":
        return f"area:{query_data['zone_id']}:conf={query_data['confidence_min']}"
    elif q_type == "Q3":
        return f"density:{query_data['zone_id']}:conf={query_data['confidence_min']}"
    elif q_type == "Q4":
        return f"compare:density:{query_data['zone_a']}:{query_data['zone_b']}:conf={query_data['confidence_min']}"
    elif q_type == "Q5":
        return f"confidence_dist:{query_data['zone_id']}:bins={query_data['bins']}"
    return "unknown_key"


def ejecutar_consulta(i, total, dist_type):
    try:
        if dist_type == "ZIPF":
            query = generar_query_zipf()
        else:
            query = generar_query_uniforme()

        cache_key = generar_cache_key(query)
        payload = {"cache_key": cache_key, "query_data": query}

        start_time = time.time()
        res = requests.post(CACHE_URL, json=payload, timeout=10)
        latency = (time.time() - start_time) * 1000

        if res.status_code == 200:
            data = res.json()
            print(
                "[TRÁFICO %s/%s] Respuesta en %s ms (fuente: %s)"
                % (i, total, latency, data.get("source"))
            )
        else:
            print("error al enviar trafico a cache, ", res.status_code)

    except requests.exceptions.RequestException as e:
        print(f"[ERROR {i}/{total}] No se pudo contactar a {CACHE_URL}: {e}")
    except Exception as e:
        print(f"[ERROR {i}/{total}] Error inesperado en tráfico: {e}")


def main():
    dist_type = os.environ.get("DISTRIBUTION", "UNIFORME").upper()
    max_workers = int(os.environ.get("MAX_WORKERS", 20))

    time.sleep(10)

    TOTAL_CONSULTAS = 20000
    start_global = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i in range(1, TOTAL_CONSULTAS + 1):
            executor.submit(ejecutar_consulta, i, TOTAL_CONSULTAS, dist_type)

    total_time = time.time() - start_global
    print("Tiempo total de ejecucion: ", total_time)


if __name__ == "__main__":
    main()
