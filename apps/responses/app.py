from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import os
import json
import pandas as pd
import numpy as np

app = FastAPI()

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

redis_client = redis.Redis(
    host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True
)

ZONAS_BBOX = {
    "Z1": {
        "lat_min": -33.445,
        "lat_max": -33.420,
        "lon_min": -70.640,
        "lon_max": -70.600,
    },
    "Z2": {
        "lat_min": -33.420,
        "lat_max": -33.390,
        "lon_min": -70.600,
        "lon_max": -70.550,
    },
    "Z3": {
        "lat_min": -33.530,
        "lat_max": -33.490,
        "lon_min": -70.790,
        "lon_max": -70.740,
    },
    "Z4": {
        "lat_min": -33.460,
        "lat_max": -33.430,
        "lon_min": -70.670,
        "lon_max": -70.630,
    },
    "Z5": {
        "lat_min": -33.470,
        "lat_max": -33.430,
        "lon_min": -70.810,
        "lon_max": -70.760,
    },
}


def calculate_area_km2(bbox):
    height = (bbox["lat_max"] - bbox["lat_min"]) * 111.32
    width = (bbox["lon_max"] - bbox["lon_min"]) * 92.9
    return abs(height * width)


# calcula el area de cada zona en km^2
ZONAS_AREA = {z: calculate_area_km2(b) for z, b in ZONAS_BBOX.items()}

# cargar el dataset
try:
    df = pd.read_csv("dataset_f.csv")
    df["zone_id"] = None
    for z, bbox in ZONAS_BBOX.items():
        mask = (
            (df["latitude"] >= bbox["lat_min"])
            & (df["latitude"] <= bbox["lat_max"])
            & (df["longitude"] >= bbox["lon_min"])
            & (df["longitude"] <= bbox["lon_max"])
        )
        df.loc[mask, "zone_id"] = z

    print(f"[INFO] Distribución por zona:\n{df['zone_id'].value_counts().to_string()}")

except FileNotFoundError:
    print("[ERROR] dataset_f.csv no encontrado.")
    exit(1)


class QueryPayload(BaseModel):
    cache_key: str
    query_data: dict


# agrega data basura a la respuesta
def increase_size(resultado, target_kb=200):
    target_bytes = target_kb * 1024
    current_json = json.dumps(resultado)
    current_size = len(current_json.encode("utf-8"))

    if current_size < target_bytes:
        needed_bytes = target_bytes - current_size - 25
        if needed_bytes > 0:
            resultado["garbage_padding"] = "x" * needed_bytes
    return resultado


def get_zona(zone_id, conf_min=0.0):
    return df.query("zone_id == @zone_id and confidence >= @conf_min")


def get_density(zone_id, conf_min):
    count = len(get_zona(zone_id, conf_min))
    area_km2 = ZONAS_AREA.get(zone_id, 0.0)
    return count / area_km2 if area_km2 > 0 else 0.0


@app.post("/process")
def process_query(payload: QueryPayload):
    cache_key = payload.cache_key
    query_data = payload.query_data
    q_type = query_data.get("query")
    zone_id = query_data.get("zone_id")
    conf_min = query_data.get("confidence_min", 0.0)

    try:
        if q_type == "Q1":
            resultado = {"count": len(get_zona(zone_id, conf_min))}

        elif q_type == "Q2":
            subset = get_zona(zone_id, conf_min)
            resultado = {
                "avg_area": (
                    float(subset["area_in_meters"].mean()) if len(subset) > 0 else 0.0
                ),
                "total_area": (
                    float(subset["area_in_meters"].sum()) if len(subset) > 0 else 0.0
                ),
                "n": len(subset),
            }

        elif q_type == "Q3":
            resultado = {"density": float(get_density(zone_id, conf_min))}

        elif q_type == "Q4":
            zone_a = query_data.get("zone_a")
            zone_b = query_data.get("zone_b")
            den_a = get_density(zone_a, conf_min)
            den_b = get_density(zone_b, conf_min)
            resultado = {
                "zone_a": float(den_a),
                "zone_b": float(den_b),
                "winner": zone_a if den_a >= den_b else zone_b,
            }

        elif q_type == "Q5":
            bins = int(query_data.get("bins", 5))
            scores = df.query("zone_id == @zone_id")["confidence"]
            hist, edges = np.histogram(scores, bins=bins, range=(0.0, 1.0))
            resultado = {
                "buckets": [
                    {
                        "bucket": i + 1,
                        "min": float(edges[i]),
                        "max": float(edges[i + 1]),
                        "count": int(hist[i]),
                    }
                    for i in range(bins)
                ]
            }

        else:
            resultado = {"error": "Tipo desconocido"}

    except Exception as e:
        print("Error procesando query", e)
        resultado = {"error": str(e)}

    resultado = increase_size(resultado, target_kb=50)
    redis_client.setex(cache_key, 3600, json.dumps(resultado))

    return resultado


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5000)
