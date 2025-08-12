# -*- coding: utf-8 -*-
"""
DAG: spotify_artist_genres_to_kafka
1) Читает последний playlist_*.json из RAW_JSON_DIR
2) Собирает уникальные artist_ids
3) Запрашивает /v1/artists?ids=... (<=50 за вызов)
4) Отправляет JSONEachRow в Kafka-топик ravelytics.spotify.artists
"""

import os
import json
import datetime
from typing import List, Dict

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Конфиг из окружения
RAW_JSON_DIR = os.getenv("RAW_JSON_DIR", "/opt/airflow/data/raw/spotify")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
ARTISTS_TOPIC = os.getenv("KAFKA_TOPIC_SPOTIFY_ARTISTS", "ravelytics.spotify.artists")

SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_ARTISTS_URL = "https://api.spotify.com/v1/artists"

# Берём client_id/secret из окружения (.env)
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")


def get_spotify_token() -> str:
    """Client Credentials токен для публичных эндпоинтов."""
    resp = requests.post(
        SPOTIFY_TOKEN_URL,
        data={"grant_type": "client_credentials"},
        auth=(CLIENT_ID, CLIENT_SECRET),
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def latest_json_path(dir_path: str) -> str:
    """Самый свежий файл playlist_*.json в RAW_JSON_DIR."""
    try:
        files = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if f.endswith(".json")]
    except FileNotFoundError:
        raise AirflowSkipException(f"Directory not found: {dir_path}")
    if not files:
        raise AirflowSkipException(f"No JSON files in {dir_path}")
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]


def ensure_topic(topic: str, broker: str) -> None:
    """Создать Kafka-топик, если не существует (удобно в dev)."""
    admin = AdminClient({"bootstrap.servers": broker})
    md = admin.list_topics(timeout=5)
    if topic in md.topics and md.topics[topic].error is None:
        print(f"[ravelytics] Topic '{topic}' exists")
        return
    fs = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    try:
        fs[topic].result()
        print(f"[ravelytics] Topic '{topic}' created")
    except Exception as e:
        print(f"[ravelytics] create_topics: {e}")


def chunked(seq: List[str], size: int = 50):
    """Пачки по 50 id (лимит Spotify)."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def fetch_artists_details(artist_ids: List[str], token: str) -> List[Dict]:
    """Вызовы /v1/artists?ids=... возвращают список artists[]."""
    headers = {"Authorization": f"Bearer {token}"}
    out = []
    for batch in chunked(artist_ids, 50):
        resp = requests.get(SPOTIFY_ARTISTS_URL, params={"ids": ",".join(batch)}, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json().get("artists", [])
        out.extend(data)
        print(f"[ravelytics] fetched {len(data)} artists (total={len(out)})")
    return out


def to_records(artists: List[Dict], ingest_ts: str) -> List[Dict]:
    """Нормализуем поля артиста под Kafka/ClickHouse."""
    records = []
    for a in artists:
        if not a or not a.get("id"):
            continue
        followers = (a.get("followers") or {}).get("total") or 0
        rec = {
            "ingest_ts": ingest_ts,
            "artist_id": a["id"],
            "artist_name": (a.get("name") or "").strip(),
            "genres": a.get("genres") or [],
            "popularity": int(a.get("popularity") or 0),
            "followers_total": int(followers),
        }
        records.append(rec)
    return records


def produce(records: List[Dict], broker: str, topic: str) -> int:
    """Отправляем JSONEachRow, key=artist_id."""
    delivered, errors = 0, []

    def cb(err, msg):
        nonlocal delivered, errors
        if err is None:
            delivered += 1
        else:
            errors.append(str(err))

    p = Producer({"bootstrap.servers": broker})
    for r in records:
        key = r["artist_id"]
        p.produce(topic, value=json.dumps(r, ensure_ascii=False).encode("utf-8"), key=key, callback=cb)
        p.poll(0)
    p.flush(10)
    if errors:
        raise RuntimeError(f"Kafka errors: {errors[:3]}")
    return delivered


def send_artist_genres_to_kafka():
    """Основная функция таска."""
    ingest_ts = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    latest = latest_json_path(RAW_JSON_DIR)
    print(f"[ravelytics] source JSON = {latest}")

    with open(latest, "r", encoding="utf-8") as f:
        items = json.load(f)

    if not isinstance(items, list) or not items:
        raise AirflowSkipException("Empty or invalid JSON structure")

    # Собираем уникальные artist_ids из плейлиста
    ids = []
    for it in items:
        track = it.get("track") or {}
        artists = track.get("artists") or []
        for a in artists:
            aid = (a.get("id") or "").strip()
            if aid:
                ids.append(aid)
    uniq_ids = sorted(set(ids))
    if not uniq_ids:
        raise AirflowSkipException("No artist ids found")

    token = get_spotify_token()
    details = fetch_artists_details(uniq_ids, token)
    records = to_records(details, ingest_ts)
    if not records:
        raise AirflowSkipException("No artist records to send")

    ensure_topic(ARTISTS_TOPIC, KAFKA_BROKER)
    sent = produce(records, KAFKA_BROKER, ARTISTS_TOPIC)
    print(f"[ravelytics] sent {sent} artist records → {ARTISTS_TOPIC}")


default_args = {
    "owner": "ravelytics",
    "start_date": datetime.datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    dag_id="spotify_artist_genres_to_kafka",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["ravelytics", "spotify", "artists", "genres"],
) as dag:
    PythonOperator(
        task_id="send_artist_genres_to_kafka",
        python_callable=send_artist_genres_to_kafka,
    )
