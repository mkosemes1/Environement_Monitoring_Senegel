"""
store_to_minio.py — Pipeline Sénégal v2
Consumer Kafka → double-bucket MinIO, 14 régions, double source.
Stockage au format PARQUET (partitionné par région/source/date).
"""

import io, json, time, logging
import pandas as pd
from datetime import datetime, timezone
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from minio import Minio
from minio.error import S3Error

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("storage-worker")

KAFKA_BROKERS   = ["kafka1:9092", "kafka2:9092", "kafka3:9092"]
KAFKA_TOPIC     = "senegal-meteo"
KAFKA_GROUP_ID  = "storage-worker-v2"
MINIO_ENDPOINT  = "minio:9000"
MINIO_ACCESS    = "minioadmin"
MINIO_SECRET    = "minioadmin"
BUCKET_RAW      = "raw-data"
BUCKET_PROC     = "processed-data"
MAX_RETRY = 10; RETRY_DELAY = 6

RULES = {
    "temp":       (-5.0,  55.0),
    "hum":        (0.0,   100.0),
    "pressure":   (900.0, 1100.0),
    "wind_speed": (0.0,   200.0),
}

#la fonction build_minio() crée une connexion au serveur MinIO et s'assure que les buckets nécessaires existent.
def build_minio():
    c = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
    for b in (BUCKET_RAW, BUCKET_PROC):
        if not c.bucket_exists(b):
            c.make_bucket(b)
            logger.info("Bucket créé : %s", b)
    return c

# La fonction obj_key() génère une clé d'objet pour le stockage dans MinIO, 
# en utilisant la région, la source et un timestamp pour créer une structure de dossiers partitionnée.
def obj_key(region_code: str, source: str, ts: str) -> str:
    try:
        dt = datetime.fromisoformat(ts.rstrip("Z"))
    except Exception:
        dt = datetime.now(timezone.utc)
    return (f"{region_code}/{source}/"
            f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
            f"{dt.strftime('%H%M%S')}_{dt.microsecond}.parquet")

# La fonction upload_parquet() prend un dictionnaire de données, le convertit en DataFrame Pandas,
def upload_parquet(client, bucket, key, data: dict):
    df = pd.DataFrame([data])
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    size = buf.getbuffer().nbytes
    client.put_object(bucket, key, buf, size, content_type="application/octet-stream")
    logger.info(" s3://%s/%s (%d bytes)", bucket, key, size)

# La fonction validate() vérifie que les données respectent les règles définies dans RULES,
def validate(raw: dict) -> dict | None:
    errors = []
    for field, (lo, hi) in RULES.items():
        val = raw.get(field)
        if val is None:
            continue
        if not (lo <= float(val) <= hi):
            errors.append(f"{field}={val} hors [{lo},{hi}]")
    if "temp" not in raw or raw["temp"] is None:
        errors.append("temp manquant")
    if errors:
        logger.warning(" Rejeté [%s/%s] : %s", raw.get("region"), raw.get("source"), errors)
        return None

    return {
        **raw,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "quality_flag": "OK",
        "temp":       round(float(raw["temp"]), 2),
        "hum":        round(float(raw["hum"]), 2) if raw.get("hum") is not None else None,
        "pressure":   round(float(raw["pressure"]), 2) if raw.get("pressure") else None,
        "wind_speed": round(float(raw["wind_speed"]), 2) if raw.get("wind_speed") else None,
    }


def build_consumer():
    for i in range(1, MAX_RETRY + 1):
        try:
            c = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms=-1,
                session_timeout_ms=30_000,
            )
            logger.info(" Consumer connecté")
            return c
        except NoBrokersAvailable:
            logger.warning("  NoBrokers — %d/%d", i, MAX_RETRY)
            time.sleep(RETRY_DELAY)
    raise RuntimeError("Kafka inaccessible.")

# Boucle principale : consomme, valide, stocke dans MinIO (raw + proc)
def main():
    logger.info(" Storage Worker démarré (format Parquet)")
    minio = build_minio()
    consumer = build_consumer()
    try:
        for msg in consumer:
            raw = msg.value
            region_code = raw.get("region_code", "XX")
            source      = raw.get("source", "unknown")
            ts          = raw.get("time", datetime.now(timezone.utc).isoformat())

            try:
                upload_parquet(minio, BUCKET_RAW, obj_key(region_code, source, ts), raw)
            except S3Error as e:
                logger.error("MinIO raw error: %s", e)

            proc = validate(raw)
            if proc:
                try:
                    upload_parquet(minio, BUCKET_PROC, obj_key(region_code, source, ts), proc)
                except S3Error as e:
                    logger.error("MinIO proc error: %s", e)
    except KeyboardInterrupt:
        logger.info(" Arrêt.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
