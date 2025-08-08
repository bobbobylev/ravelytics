import os
import json
import datetime
from typing import List, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from confluent_kafka import Producer


RAW_JSON_DIR = os.getenv("RAW_JSON_DIR", "/opt/airflow/data/raw/spotify")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC_SPOTIFY_TRACKS", "ravelytics.spotify.tracks")


def _latest_json_path(dir_path: str) -> str:
    files = [
        os.path.join(dir_path, f)
        for f in os.listdir(dir_path)
        if f.endswith(".json")
    ]
    if not files:
        raise AirflowSkipException(f"No JSON files found in {dir_path}")
    # Берём самый свежий по времени изменения
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]


def _record_from_item(item: dict, ingest_ts: str) -> Dict:
    """Готовим одну JSON-запись из элемента плейлиста (items[i])."""
    track      = item.get("track") or {}
    album      = track.get("album") or {}
    artists    = track.get("artists") or []

    return {
        "ingest_ts": ingest_ts,  # ISO-строка
        "track_id": track.get("id") or "",
        "track_name": track.get("name") or "",
        "artists": [a.get("name") or "" for a in artists],
        "artist_ids": [a.get("id") or "" for a in artists],
        "album_id": album.get("id") or "",
        "album_name": album.get("name") or "",
        # release_date может прийти как YYYY-MM-DD или YYYY, приведём к дате грубо
        "release_date": (album.get("release_date") or "1970-01-01"),
        "popularity": int(track.get("popularity") or 0),
        "added_at": (item.get("added_at") or ingest_ts),
        "external_url": (track.get("external_urls") or {}).get("spotify", ""),
    }


def _produce_to_kafka(records: List[Dict], broker: str, topic: str) -> int:
    delivered = 0
    errors = []

    def _cb(err, msg):
        nonlocal delivered, errors
        if err is None:
            delivered += 1
        else:
            errors.append(str(err))

    p = Producer({"bootstrap.servers": broker})
    for rec in records:
        p.produce(topic, json.dumps(rec).encode("utf-8"), callback=_cb)
    p.flush(10)

    if errors:
        raise RuntimeError(f"Kafka delivery errors: {errors[:3]}")
    return delivered


def send_metadata_to_kafka():
    ingest_ts = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    latest = _latest_json_path(RAW_JSON_DIR)
    print(f"[ravelytics] Using latest JSON: {latest}")

    with open(latest, "r", encoding="utf-8") as f:
        items = json.load(f)

    if not isinstance(items, list) or not items:
        raise AirflowSkipException("Empty or invalid JSON structure")

    records = [_record_from_item(it, ingest_ts) for it in items]
    # Фильтруем пустые id на всякий
    records = [r for r in records if r["track_id"]]

    if not records:
        raise AirflowSkipException("No valid records to send")

    sent = _produce_to_kafka(records, KAFKA_BROKER, KAFKA_TOPIC)
    print(f"[ravelytics] Sent {sent} records to topic {KAFKA_TOPIC} at {KAFKA_BROKER}")


default_args = {
    "owner": "ravelytics",
    "start_date": datetime.datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    dag_id="spotify_metadata_to_kafka",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["ravelytics", "spotify", "metadata"],
) as dag:

    send_task = PythonOperator(
        task_id="send_metadata_to_kafka",
        python_callable=send_metadata_to_kafka,
    )
