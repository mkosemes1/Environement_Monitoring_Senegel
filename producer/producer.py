"""
producer.py — Dakar Environmental Monitoring Pipeline v2
Double source : Open-Meteo (principal) + OpenWeatherMap (secondaire)
Couvre les 14 régions administratives du Sénégal.
"""

import json
import time
import logging
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import os

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("producer")

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BROKERS   = ["kafka1:9092", "kafka2:9092", "kafka3:9092"]
KAFKA_TOPIC     = "senegal-meteo"
FETCH_INTERVAL  = 60        # secondes entre chaque cycle de collecte
MAX_RETRY       = 15
RETRY_DELAY     = 6

# Clé OpenWeatherMap (gratuit, 1000 req/jour, sans CB)
OWM_API_KEY = os.environ.get("OWM_API_KEY", "")

# ── 14 Régions administratives du Sénégal ─────────────────────────────────────
# Source des coordonnées : données géographiques officielles IGN/ANSD Sénégal
REGIONS = [
    {"name": "Dakar",       "lat": 14.6928, "lon": -17.4467, "code": "DK"},
    {"name": "Thiès",       "lat": 14.7886, "lon": -16.9255, "code": "TH"},
    {"name": "Diourbel",    "lat": 14.6554, "lon": -16.2322, "code": "DB"},
    {"name": "Fatick",      "lat": 14.3390, "lon": -16.4114, "code": "FK"},
    {"name": "Kaolack",     "lat": 14.1652, "lon": -16.0726, "code": "KL"},
    {"name": "Kaffrine",    "lat": 14.1059, "lon": -15.5503, "code": "KF"},
    {"name": "Tambacounda", "lat": 13.7707, "lon": -13.6673, "code": "TC"},
    {"name": "Kédougou",    "lat": 12.5559, "lon": -12.1747, "code": "KD"},
    {"name": "Kolda",       "lat": 12.8989, "lon": -14.9413, "code": "KO"},
    {"name": "Sédhiou",     "lat": 12.7080, "lon": -15.5569, "code": "SD"},
    {"name": "Ziguinchor",  "lat": 12.5681, "lon": -16.2719, "code": "ZG"},
    {"name": "Saint-Louis", "lat": 16.0179, "lon": -16.4896, "code": "SL"},
    {"name": "Louga",       "lat": 15.6179, "lon": -16.2241, "code": "LG"},
    {"name": "Matam",       "lat": 15.6559, "lon": -13.2557, "code": "MT"},
]


# ── Open-Meteo : données courantes ─────────────────────────────────────────────
def fetch_open_meteo(region: dict) -> dict | None:
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={region['lat']}&longitude={region['lon']}"
        "&current=temperature_2m,relative_humidity_2m,surface_pressure,"
        "wind_speed_10m,wind_direction_10m,weather_code,apparent_temperature"
        "&timezone=Africa%2FDakar"
    )

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        raw = resp.json()
        c = raw.get("current", {})
        return {
            "source":       "open-meteo",
            "time":         datetime.now(timezone.utc).isoformat(),
            "region":       region["name"],
            "region_code":  region["code"],
            "lat":          region["lat"],
            "lon":          region["lon"],
            "temp":         c.get("temperature_2m"),
            "temp_feels":   c.get("apparent_temperature"),
            "hum":          c.get("relative_humidity_2m"),
            "pressure":     c.get("surface_pressure"),
            "wind_speed":   c.get("wind_speed_10m"),
            "wind_dir":     c.get("wind_direction_10m"),
            "weather_code": c.get("weather_code"),
        }
    except Exception as exc:
        logger.error("[Open-Meteo] %s — %s", region["name"], exc)
        return None


# ── OpenWeatherMap : données courantes ─────────────────────────────────────────
def fetch_owm(region: dict) -> dict | None:
    if OWM_API_KEY == "":
        return None   # clé non configurée, on passe
    url = (
        "https://api.openweathermap.org/data/2.5/weather"
        f"?lat={region['lat']}&lon={region['lon']}"
        f"&appid={OWM_API_KEY}&units=metric&lang=fr"
    )
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        raw = resp.json()
        main = raw.get("main", {})
        wind = raw.get("wind", {})
        desc = raw.get("weather", [{}])[0]
        return {
            "source":         "openweathermap",
            "time":           datetime.now(timezone.utc).isoformat(),
            "region":         region["name"],
            "region_code":    region["code"],
            "lat":            region["lat"],
            "lon":            region["lon"],
            "temp":           main.get("temp"),
            "temp_feels":     main.get("feels_like"),
            "hum":            main.get("humidity"),
            "pressure":       main.get("pressure"),
            "wind_speed":     wind.get("speed"),
            "wind_dir":       wind.get("deg"),
            "weather_code":   desc.get("id"),
            "weather_label":  desc.get("description", ""),
        }
    except Exception as exc:
        logger.error("[OWM] %s — %s", region["name"], exc)
        return None


# ── Fusion des deux sources ────────────────────────────────────────────────────
def fetch_region(region: dict) -> list[dict]:
    """Interroge les deux APIs pour une région et retourne les messages valides."""
    results = []
    m1 = fetch_open_meteo(region)
    if m1:
        results.append(m1)
    m2 = fetch_owm(region)
    if m2:
        results.append(m2)
    return results


# ── Kafka Producer ─────────────────────────────────────────────────────────────
def build_producer() -> KafkaProducer:
    for attempt in range(1, MAX_RETRY + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=50,
                request_timeout_ms=30_000,
            )
            logger.info(" Kafka connecté : %s", KAFKA_BROKERS)
            return producer
        except NoBrokersAvailable:
            logger.warning("  NoBrokersAvailable — tentative %d/%d", attempt, MAX_RETRY)
            time.sleep(RETRY_DELAY)
    raise RuntimeError("Kafka inaccessible après %d tentatives." % MAX_RETRY)


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    logger.info(" Producer démarré — %d régions × 2 sources", len(REGIONS))
    producer = build_producer()

    cycle = 0
    try:
        while True:
            cycle += 1
            total_sent = 0
            logger.info("=== Cycle #%d ===", cycle)

            for region in REGIONS:
                messages = fetch_region(region)
                for msg in messages:
                    future = producer.send(KAFKA_TOPIC, value=msg)
                    future.add_errback(lambda exc: logger.error("Kafka send error: %s", exc))
                    total_sent += 1
                    logger.info(" [%s] %s | %.1f°C | %d%% hum",
                                msg["source"], msg["region"],
                                msg.get("temp") or 0, msg.get("hum") or 0)
                # Petite pause entre régions pour éviter le rate-limit API
                time.sleep(1.5)

            producer.flush()
            logger.info(" Cycle #%d terminé — %d messages envoyés", cycle, total_sent)
            logger.info(" Prochain cycle dans %ds…", FETCH_INTERVAL)
            time.sleep(FETCH_INTERVAL)

    except KeyboardInterrupt:
        logger.info(" Arrêt du producer.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
