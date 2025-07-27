import os
import json
import datetime
import requests
from confluent_kafka import Producer
from airflow import DAG
from airflow.operators.python import PythonOperator

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
    # найдём самый свежий JSON‑файл
    files = sorted(
        [f for f in os.listdir(RAW_JSON) if f.endswith('.json')],
        reverse=True
    )
    if not files:
        raise FileNotFoundError("Нет JSON-файлов в " + RAW_JSON)
    latest = files[0]
    with open(os.path.join(RAW_JSON, latest), 'r', encoding='utf-8') as f:
        items = json.load(f)

    for item in items:
        track = item['track']
        track_id = track['id']
        preview_url = track.get('preview_url')
        if not preview_url:
            continue

        # скачиваем preview
        resp = requests.get(preview_url)
        resp.raise_for_status()
        audio_path = os.path.join(RAW_AUDIO, f"{track_id}.mp3")
        with open(audio_path, 'wb') as af:
            af.write(resp.content)

        # отправляем в Kafka метаданные + локальный путь
        msg = {
            'track_id': track_id,
            'audio_path': audio_path,
            'fetched_at': datetime.datetime.utcnow().isoformat()
        }
        produce_to_kafka(msg)

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