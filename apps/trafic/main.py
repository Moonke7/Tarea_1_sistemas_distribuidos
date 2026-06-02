import os
import time
import uuid
import json
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
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


def ejecutar_consulta(i, total, dist_type, producer):
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
            "payload": {
                "cache_key": cache_key,
                "query_data": query
            }
        }

        producer.send(KAFKA_TOPIC_QUERIES, value=message)
        if FLUSH_PER_MESSAGE:
            producer.flush()
        
        print(f"[TRÁFICO {i}/{total}] Mensaje enviado a Kafka: {message['message_id']}")

    except Exception as e:
        print(f"[ERROR {i}/{total}] Error al enviar mensaje a Kafka: {e}")


def main():
    dist_type = os.environ.get("DISTRIBUTION", "UNIFORME").upper()
    max_workers = int(os.environ.get("MAX_WORKERS", 20))

    time.sleep(10)

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    TOTAL_CONSULTAS = 20000
    start_global = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i in range(1, TOTAL_CONSULTAS + 1):
            executor.submit(ejecutar_consulta, i, TOTAL_CONSULTAS, dist_type, producer)

    total_time = time.time() - start_global
    print("Tiempo total de ejecucion: ", total_time)
    
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
