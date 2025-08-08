import os
import json
import datetime
import requests
from confluent_kafka import Producer
from airflow import DAG
from airflow.operators.python import PythonOperator
from spotify_ingest import get_spotify_token


# Папки внутри контейнера
RAW_JSON = '/opt/airflow/data/raw/spotify'
RAW_AUDIO = '/opt/airflow/data/raw/spotify/audio'
os.makedirs(RAW_AUDIO, exist_ok=True)

# Параметры
BROKER = os.getenv('KAFKA_BROKER')  # из .env, например "kafka:9092"
TOPIC = 'ravelytics.raw_audio'

def produce_to_kafka(message: dict):
    p = Producer({'bootstrap.servers': BROKER})
    p.produce(TOPIC, json.dumps(message).encode('utf-8'))
    p.flush()

def fetch_and_stream_audio():
    # получаем токен один раз
    token = get_spotify_token()
    headers = {'Authorization': f'Bearer {token}'}

    # находим самый свежий JSON-файл
    files = sorted([...], reverse=True)
    latest = files[0]
    with open(os.path.join(RAW_JSON, latest), 'r', encoding='utf-8') as f:
        items = json.load(f)

    for item in items:
        track = item['track']
        track_id = track['id']

        # 1) пытаемся взять preview из плейлиста
        preview_url = track.get('preview_url')

        # 2) если его нет — делаем прямой запрос к треку
        if not preview_url:
            detail_resp = requests.get(
                f'https://api.spotify.com/v1/tracks/{track_id}',
                headers=headers
            )
            try:
                detail_resp.raise_for_status()
                preview_url = detail_resp.json().get('preview_url')
                print(f"[ravelytics] Fallback preview_url for {track_id}: {preview_url}")
            except requests.exceptions.HTTPError as e:
                print(f"[ravelytics] Cannot fetch track details for {track_id}: {e}")
                continue  # пропускаем этот трек

        # 3) если после всего нет preview — пропускаем
        if not preview_url:
            print(f"[ravelytics] No preview for track {track_id}, skipping")
            continue

        # 4) скачиваем и сохраняем MP3
        resp = requests.get(preview_url)
        resp.raise_for_status()
        audio_path = os.path.join(RAW_AUDIO, f"{track_id}.mp3")
        with open(audio_path, 'wb') as af:
            af.write(resp.content)
        print(f"[ravelytics] Saved preview for {track_id} at {audio_path}")

        # 5) отправляем сообщение в Kafka
        msg = {
            'track_id': track_id,
            'audio_path': audio_path,
            'fetched_at': datetime.datetime.utcnow().isoformat()
        }
        produce_to_kafka(msg)
        print(f"[ravelytics] Produced message for {track_id}")

# Описание DAG
default_args = {
    'owner': 'ravelytics',
    'start_date': datetime.datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(
    'spotify_audio_ingest',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['ravelytics', 'spotify', 'audio'],
) as dag:

    task_audio = PythonOperator(
        task_id='fetch_and_stream_audio',
        python_callable=fetch_and_stream_audio,
    )