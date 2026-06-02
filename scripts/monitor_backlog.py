#!/usr/bin/env python3
"""
monitor_backlog.py — Standalone script

Muestrea el lag de Kafka para geo-queries y geo-retry periódicamente
e inserta los valores en la tabla backlog_samples de Postgres.

Uso:
    python scripts/monitor_backlog.py

Requerimientos:
    pip install kafka-python psycopg2-binary
"""

import os
import time
import psycopg2
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
)
KAFKA_TOPIC_QUERIES = "geo-queries"
KAFKA_TOPIC_RETRY = "geo-retry"
KAFKA_CONSUMER_GROUP = "geo-consumers"
SAMPLE_INTERVAL = int(os.environ.get("SAMPLE_INTERVAL", 5))

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", 5432))

PG_CONFIG = {
    "host": POSTGRES_HOST,
    "port": POSTGRES_PORT,
    "database": "metrics_db",
    "user": "sistemas_d",
    "password": "sistemas_d",
}

TOPICS = [KAFKA_TOPIC_QUERIES, KAFKA_TOPIC_RETRY]


def get_partitions(consumer, topics):
    partitions = []
    for topic in topics:
        metadata = consumer.partitions_for_topic(topic)
        if metadata is None:
            print(f"[MONITOR] Warning: topic '{topic}' no existe aún")
            continue
        for partition in metadata:
            partitions.append(TopicPartition(topic, partition))
    return partitions


def main():
    print("[MONITOR] Iniciando monitoreo de backlog...")
    print(f"[MONITOR] Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[MONITOR] Topics: {TOPICS}")
    print(f"[MONITOR] Intervalo: {SAMPLE_INTERVAL}s")
    print(f"[MONITOR] Postgres: {POSTGRES_HOST}:{POSTGRES_PORT}")

    time.sleep(2)

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id="monitor-backlog",
        )
    except NoBrokersAvailable:
        print("[MONITOR] Error: No se pudo conectar a Kafka")
        return

    try:
        temp_consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        )
    except NoBrokersAvailable:
        print("[MONITOR] Error: No se pudo conectar a Kafka (temp consumer)")
        admin_client.close()
        return

    partitions = get_partitions(temp_consumer, TOPICS)
    if not partitions:
        print("[MONITOR] No se encontraron particiones disponibles")
        temp_consumer.close()
        admin_client.close()
        return

    print(f"[MONITOR] Particiones monitoreadas: {len(partitions)}")
    for tp in partitions:
        print(f"[MONITOR]   {tp.topic} p{tp.partition}")

    while True:
        try:
            end_offsets = temp_consumer.end_offsets(partitions)

            try:
                group_offsets = admin_client.list_consumer_group_offsets(
                    KAFKA_CONSUMER_GROUP
                )
            except Exception:
                group_offsets = {}

            conn = psycopg2.connect(**PG_CONFIG)
            cursor = conn.cursor()

            for tp in partitions:
                end_offset = end_offsets.get(tp, 0)
                committed = group_offsets.get(tp)
                current_offset = committed.offset if committed is not None else 0
                lag = max(0, end_offset - current_offset)

                cursor.execute(
                    """
                    INSERT INTO backlog_samples
                        (topic, partition, consumer_group, lag)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (tp.topic, tp.partition, KAFKA_CONSUMER_GROUP, lag),
                )

                print(
                    f"[MONITOR] {tp.topic} p{tp.partition}: "
                    f"end={end_offset} current={current_offset} lag={lag}"
                )

            conn.commit()
            cursor.close()
            conn.close()

        except Exception as e:
            print(f"[MONITOR] Error en ciclo de muestreo: {e}")

        time.sleep(SAMPLE_INTERVAL)


if __name__ == "__main__":
    main()
