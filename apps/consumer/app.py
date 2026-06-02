import os
import json
import time
import uuid
import random
import requests
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
MAX_REQUESTS_PER_SECOND = int(os.environ.get("MAX_REQUESTS_PER_SECOND", 0))
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "metrics")
CACHE_URL = os.environ.get("CACHE_URL", "http://cache:5000/query")
RESPONSES_URL = os.environ.get("RESPONSES_URL", "http://responses:5000/process")

KAFKA_TOPIC_QUERIES = "geo-queries"
KAFKA_TOPIC_RETRY = "geo-retry"
KAFKA_TOPIC_DLQ = "geo-dlq"
KAFKA_CONSUMER_GROUP = "geo-consumers"

CONSUMER_ID = os.environ.get("HOSTNAME", "consumer-" + str(uuid.uuid4())[:8])

PG_CONFIG = {
    "host": POSTGRES_HOST,
    "database": "metrics_db",
    "user": "sistemas_d",
    "password": "sistemas_d",
}


def get_pg_connection():
    return psycopg2.connect(**PG_CONFIG)


def insert_kafka_metric(
    message_id,
    query_type,
    zone_id,
    cache_key,
    consumer_id,
    retry_count,
    source,
    latency_ms,
    status,
    error_reason=None,
):
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO kafka_query_metrics
                (message_id, query_type, zone_id, cache_key, consumer_id,
                 retry_count, source, latency_ms, status, error_reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                message_id,
                query_type,
                zone_id,
                cache_key,
                consumer_id,
                retry_count,
                source,
                latency_ms,
                status,
                error_reason,
            ),
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("Error al guardar metrica en DB", CONSUMER_ID, e)


def insert_dlq_log(message_id, payload, total_retries, reason):
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO dlq_log (message_id, payload, total_retries, reason)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (message_id) DO NOTHING
            """,
            (message_id, json.dumps(payload), total_retries, reason),
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("Error al guardar DLQ", CONSUMER_ID, e)


def extract_query_info(payload):
    cache_key = payload.get("cache_key", "")
    query_data = payload.get("query_data", {})
    query_type = query_data.get("query", "unknown")
    zone_id = query_data.get("zone_id", query_data.get("zone_a", ""))
    return cache_key, query_data, query_type, zone_id


def process_message(msg_value, msg_topic):
    message_id = msg_value.get("message_id", str(uuid.uuid4()))
    created_at = msg_value.get("created_at", "")
    retry_count = msg_value.get("retry_count", 0)
    payload = msg_value.get("payload", {})

    cache_key, query_data, query_type, zone_id = extract_query_info(payload)

    start_time = time.time()

    try:
        if msg_topic == KAFKA_TOPIC_QUERIES:
            source = "cache"
            res = requests.post(CACHE_URL, json=payload, timeout=10)
        else:
            source = "retry-direct"
            res = requests.post(RESPONSES_URL, json=payload, timeout=10)

        latency_ms = (time.time() - start_time) * 1000

        if res.status_code == 200:
            insert_kafka_metric(
                message_id,
                query_type,
                zone_id,
                cache_key,
                CONSUMER_ID,
                retry_count,
                source,
                latency_ms,
                status="success",
            )
            return True
        else:
            raise Exception(f"HTTP {res.status_code}")

    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        error_reason = str(e)

        insert_kafka_metric(
            message_id,
            query_type,
            zone_id,
            cache_key,
            CONSUMER_ID,
            retry_count,
            source,
            latency_ms,
            status="retry",
            error_reason=error_reason,
        )

        retry_count += 1
        if retry_count >= MAX_RETRIES:
            dlq_data = {
                "message_id": message_id,
                "created_at": created_at,
                "retry_count": retry_count,
                "payload": payload,
            }
            producer.send(KAFKA_TOPIC_DLQ, value=dlq_data)
            producer.flush()
            insert_dlq_log(
                message_id,
                payload,
                retry_count,
                f"Max retries ({MAX_RETRIES}) reached: {error_reason}",
            )
            print(f"DLQ: {message_id} " f"despues de: {retry_count} intentos")
        else:
            retry_msg = {
                "message_id": message_id,
                "created_at": created_at,
                "retry_count": retry_count,
                "payload": payload,
            }
            producer.send(KAFKA_TOPIC_RETRY, value=retry_msg)
            producer.flush()
            print(f"RETRY {retry_count}/{MAX_RETRIES}: {message_id}")

        return False


def main():
    global producer

    print("Iniciando consumidor")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topics: {KAFKA_TOPIC_QUERIES}, {KAFKA_TOPIC_RETRY}")
    print(
        f"MAX_RETRIES={MAX_RETRIES}, "
        f"MAX_REQUESTS_PER_SECOND={MAX_REQUESTS_PER_SECOND}"
    )

    time.sleep(15)

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_QUERIES,
            KAFKA_TOPIC_RETRY,
            group_id=KAFKA_CONSUMER_GROUP,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            max_poll_records=1,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
    except NoBrokersAvailable:
        print("Error: No se pudo conectar a Kafka")
        return

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda m: json.dumps(m).encode("utf-8"),
        )
    except NoBrokersAvailable:
        print("Error: No se pudo conectar a Kafka")
        consumer.close()
        return

    print("Conectado... esperando mensajes...")

    rate_limit_interval = (
        1.0 / MAX_REQUESTS_PER_SECOND if MAX_REQUESTS_PER_SECOND > 0 else 0
    )

    for msg in consumer:
        msg_start = time.time()

        try:
            msg_value = msg.value
            msg_topic = msg.topic
        except Exception as e:
            print("Error parseando mensaje", e)
            continue

        print(
            f"Recibido de '{msg_topic}': {msg_value.get('message_id', 'unknown')[:12]}..."
        )

        process_message(msg_value, msg_topic)

        if rate_limit_interval > 0:
            elapsed = time.time() - msg_start
            sleep_time = rate_limit_interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    consumer.close()


if __name__ == "__main__":
    main()
