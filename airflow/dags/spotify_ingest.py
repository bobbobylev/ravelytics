# airflow/dags/spotify_ingest.py

import os
import json
import datetime
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

# Папка для «сырых» данных
RAW_PATH = '/opt/airflow/data/raw/spotify'
os.makedirs(RAW_PATH, exist_ok=True)

# Параметры Spotify API
SPOTIFY_TOKEN_URL    = 'https://accounts.spotify.com/api/token'
SPOTIFY_PLAYLIST_URL = 'https://api.spotify.com/v1/playlists/{playlist_id}/tracks'
PLAYLIST_ID          = '37i9dQZF1DX4dyzvuaRJ0n'  # заменишь на свой

def get_spotify_token():
    """Получаем OAuth‑токен (client_credentials)."""
    client_id     = os.getenv('SPOTIFY_CLIENT_ID')
    client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
    resp = requests.post(
        SPOTIFY_TOKEN_URL,
        data={'grant_type': 'client_credentials'},
        auth=(client_id, client_secret)
    )
    resp.raise_for_status()
    token = resp.json()['access_token']
    print(f"[ravelytics] Obtained token (len={len(token)})")
    return token

def fetch_playlist_tracks():
    """Скачиваем все треки из плейлиста и сохраняем в JSON."""
    token   = get_spotify_token()
    headers = {'Authorization': f'Bearer {token}'}

    url      = SPOTIFY_PLAYLIST_URL.format(playlist_id=PLAYLIST_ID)
    params   = {'limit': 100}
    all_items = []

    # пагинация
    while url:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        items = data.get('items', [])
        print(f"[ravelytics] Fetched {len(items)} items; next={data.get('next')}")
        all_items.extend(items)
        url = data.get('next')

    # сохраняем в файл с датой
    today    = datetime.datetime.utcnow().strftime('%Y-%m-%d')
    filename = f"playlist_{PLAYLIST_ID}_{today}.json"
    out_path = os.path.join(RAW_PATH, filename)
    print(f"[ravelytics] Writing {len(all_items)} items to {out_path}")
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(all_items, f, ensure_ascii=False, indent=2)
    print("[ravelytics] Successfully wrote JSON file")

# Параметры DAG
default_args = {
    'owner':        'ravelytics',
    'start_date':   datetime.datetime(2025, 1, 1),
    'retries':      1,
    'retry_delay':  datetime.timedelta(minutes=5),
}

with DAG(
    dag_id='spotify_ingest',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['ravelytics', 'spotify'],
) as dag:

    task_fetch = PythonOperator(
        task_id='fetch_playlist_tracks',
        python_callable=fetch_playlist_tracks,
    )
