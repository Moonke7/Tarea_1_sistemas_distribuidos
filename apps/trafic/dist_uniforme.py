import random

ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERIES = ["Q1", "Q2", "Q3", "Q4", "Q5"]


def generar_query_uniforme():
    q_type = random.choice(QUERIES)
    confidence_min = round(random.uniform(0.0, 1.0), 2)

    if q_type == "Q4":
        zona_a = random.choice(ZONAS)
        zona_b = random.choice(ZONAS)
        while zona_b == zona_a:
            zona_b = random.choice(ZONAS)

        return {
            "query": q_type,
            "zone_a": zona_a,
            "zone_b": zona_b,
            "confidence_min": confidence_min,
        }
    elif q_type == "Q5":
        zona = random.choice(ZONAS)
        bins = random.randint(2, 10)

        return {"query": q_type, "zone_id": zona, "bins": bins}
    else:
        zona = random.choice(ZONAS)

        return {"query": q_type, "zone_id": zona, "confidence_min": confidence_min}
