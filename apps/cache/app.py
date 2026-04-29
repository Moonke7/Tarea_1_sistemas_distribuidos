from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import redis
import requests
import os
import json
import psycopg2
import time

app = FastAPI()

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
RESPONSES_URL = os.environ.get("RESPONSES_URL", "http://responses:5000/process")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "metrics")

redis_client = redis.Redis(
    host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True
)


class QueryPayload(BaseModel):
    cache_key: str
    query_data: dict


def log_metric(q_type, zone_id, cache_key, source, latency_ms):
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database="metrics_db",
            user="sistemas_d",
            password="sistemas_d",
        )
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO query_metrics (query_type, zone_id, cache_key, source, latency_ms)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (q_type, zone_id, cache_key, source, latency_ms),
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("error al guardar métricas en db", e)


@app.post("/query")
def handle_query(payload: QueryPayload, background_tasks: BackgroundTasks):
    cache_key = payload.cache_key
    query_data = payload.query_data
    q_type = query_data.get("query")

    start_time = time.time()
    try:
        cached_result = redis_client.get(cache_key)
    except redis.exceptions.ConnectionError:
        print("error al contectar en cache, saliendo..")
        exit(1)

    if cached_result:
        print("🦍 HIT de cache")
        latency_ms = (time.time() - start_time) * 1000
        zone_id = query_data.get("zone_id", query_data.get("zone_a", ""))
        background_tasks.add_task(
            log_metric, q_type, zone_id, cache_key, "cache", latency_ms
        )
        return {"source": "cache", "data": json.loads(cached_result)}

    print("🦧 MISS de cache")
    try:
        res = requests.post(RESPONSES_URL, json=payload.model_dump(), timeout=10)

        if res.status_code == 200:
            latency_ms = (time.time() - start_time) * 1000
            zone_id = query_data.get("zone_id", query_data.get("zone_a", ""))
            background_tasks.add_task(
                log_metric, q_type, zone_id, cache_key, "responses", latency_ms
            )
            return {"source": "responses", "data": res.json()}
        else:
            raise HTTPException(status_code=502, detail="Error en servicio responses")

    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=503, detail="Servicio responses no disponible")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5001)
