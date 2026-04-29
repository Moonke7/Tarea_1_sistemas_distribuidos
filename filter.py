import csv

INPUT_FILE = "dataset.csv"
OUTPUT_FILE = "dataset_f.csv"

# Columnas que nos interesan (en ese orden en el CSV de salida)
KEEP_COLS = ["latitude", "longitude", "area_in_meters", "confidence"]

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


def in_any_zone(lat: float, lon: float) -> bool:
    for bbox in ZONAS_BBOX.values():
        if (
            bbox["lat_min"] <= lat <= bbox["lat_max"]
            and bbox["lon_min"] <= lon <= bbox["lon_max"]
        ):
            return True
    return False


def filter_dataset():
    rows_read = 0
    rows_written = 0

    try:
        with (
            open(INPUT_FILE, "r", encoding="utf-8", newline="") as f_in,
            open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f_out,
        ):
            reader = csv.DictReader(f_in)

            # Verificar que las columnas requeridas existen
            missing = [c for c in KEEP_COLS if c not in reader.fieldnames]
            if missing:
                print(f"[ERROR] Columnas no encontradas en el CSV: {missing}")
                print(f"        Columnas disponibles: {reader.fieldnames}")
                return

            writer = csv.DictWriter(f_out, fieldnames=KEEP_COLS, extrasaction="ignore")
            writer.writeheader()

            for row in reader:
                rows_read += 1

                if rows_read % 1_000_000 == 0:
                    print(
                        f"  Procesadas {rows_read:,} filas — guardadas: {rows_written:,}"
                    )

                try:
                    lat = float(row["latitude"])
                    lon = float(row["longitude"])
                except (ValueError, KeyError):
                    continue  # fila con datos corruptos, saltar

                if in_any_zone(lat, lon):
                    writer.writerow(row)
                    rows_written += 1

        print(f"\n✅ Filtrado completo.")
        print(f"   Filas leídas  : {rows_read:,}")
        print(f"   Filas escritas: {rows_written:,}  →  {OUTPUT_FILE}")

    except FileNotFoundError:
        print(f"[ERROR] No se encontró el archivo '{INPUT_FILE}'.")
    except Exception as e:
        print(f"[ERROR] {e}")


if __name__ == "__main__":
    print(f"📂 Leyendo '{INPUT_FILE}' y filtrando por zonas geográficas...")
    print(f"   Zonas: {', '.join(ZONAS_BBOX.keys())}")
    print(f"   Columnas a conservar: {KEEP_COLS}\n")
    filter_dataset()
