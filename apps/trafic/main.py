from asyncio import sleep
import os
import time
import uuid
import json
from datetime import datetime, timezone
from kafka import KafkaProducer
from dist_uniforme import generar_query_uniforme
from dist_zipf import generar_query_zipf

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_QUERIES = os.environ.get("KAFKA_TOPIC_QUERIES", "geo-queries")
FLUSH_PER_MESSAGE = os.environ.get("FLUSH_PER_MESSAGE", "true").lower() == "true"


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


def ejecutar_consulta(i, total, dist_type, producer, do_flush=True):
    try:
        if dist_type == "ZIPF":
            query = generar_query_zipf()
        else:
            query = generar_query_uniforme()

        cache_key = generar_cache_key(query)

        message = {
            "message_id": str(uuid.uuid4()),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "retry_count": 0,
            "payload": {"cache_key": cache_key, "query_data": query},
        }

        producer.send(KAFKA_TOPIC_QUERIES, value=message)
        if do_flush:
            producer.flush()

        print(f"[TRÁFICO {i}/{total}] Mensaje enviado a Kafka: {message['message_id']}")

    except Exception as e:
        print(f"[ERROR {i}/{total}] Error al enviar mensaje a Kafka: {e}")


def main():
    dist_type = os.environ.get("DISTRIBUTION", "UNIFORME").upper()

    TOTAL_CONSULTAS = 1500
    BASE_SLEEP = float(os.environ.get("BASE_SLEEP", 0.02))
    SPIKE_MODE = os.environ.get("SPIKE_MODE", "false").lower() == "true"
    SPIKE_START_DELAY = float(os.environ.get("SPIKE_START_DELAY", 15))
    SPIKE_DURATION = float(os.environ.get("SPIKE_DURATION", 10))
    SPIKE_SLEEP = float(os.environ.get("SPIKE_SLEEP", 0.002))
    SPIKE_FLUSH_DISABLE = os.environ.get("SPIKE_FLUSH_DISABLE", "true").lower() == "true"

    print(f"[TRÁFICO] TOTAL_CONSULTAS={TOTAL_CONSULTAS}, BASE_SLEEP={BASE_SLEEP}")
    print(f"[TRÁFICO] SPIKE_MODE={SPIKE_MODE}, SPIKE_START_DELAY={SPIKE_START_DELAY}s, "
          f"SPIKE_DURATION={SPIKE_DURATION}s, SPIKE_SLEEP={SPIKE_SLEEP}s, "
          f"SPIKE_FLUSH_DISABLE={SPIKE_FLUSH_DISABLE}")

    time.sleep(20)

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    start_global = time.time()

    for i in range(1, TOTAL_CONSULTAS + 1):
        elapsed = time.time() - start_global
        in_spike = SPIKE_MODE and SPIKE_START_DELAY <= elapsed < SPIKE_START_DELAY + SPIKE_DURATION
        do_flush = FLUSH_PER_MESSAGE and not (in_spike and SPIKE_FLUSH_DISABLE)

        ejecutar_consulta(i, TOTAL_CONSULTAS, dist_type, producer, do_flush=do_flush)

        if in_spike:
            current_sleep = SPIKE_SLEEP
        else:
            current_sleep = BASE_SLEEP
        time.sleep(current_sleep)

    total_time = time.time() - start_global
    print("Tiempo total de ejecucion: ", total_time)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
