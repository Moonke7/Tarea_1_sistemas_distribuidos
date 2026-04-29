import random

ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]
PROBABILIDADES_ZONAS = [0.43, 0.25, 0.16, 0.10, 0.06]

QUERIES = ["Q1", "Q2", "Q3", "Q4", "Q5"]


def seleccionar_zona_zipf():
    return random.choices(ZONAS, weights=PROBABILIDADES_ZONAS, k=1)[0]


def generar_query_zipf():
    q_type = random.choice(QUERIES)
    confidence_min = round(random.uniform(0.0, 1.0), 2)

    if q_type == "Q4":
        zona_a = seleccionar_zona_zipf()
        zona_b = seleccionar_zona_zipf()
        while zona_b == zona_a:
            zona_b = seleccionar_zona_zipf()

        return {
            "query": q_type,
            "zone_a": zona_a,
            "zone_b": zona_b,
            "confidence_min": confidence_min,
        }
    elif q_type == "Q5":
        zona = seleccionar_zona_zipf()
        bins = random.randint(2, 10)

        return {"query": q_type, "zone_id": zona, "bins": bins}
    else:
        zona = seleccionar_zona_zipf()

        return {"query": q_type, "zone_id": zona, "confidence_min": confidence_min}
