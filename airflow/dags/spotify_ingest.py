# Импорт необходимых модулей:
# os, json, datetime для работы с файловой системой, сериализации и времени
# requests для HTTP-запросов к API Spotify
# DAG и PythonOperator из Airflow для описания и выполнения задач
# AirflowSkipException для корректного пропуска задач при отсутствии данных
import os
import json
import datetime
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

# ------------------- Конфигурация через .env -------------------
# Путь, где будут сохраняться JSON-файлы с треками
RAW_PATH = '/opt/airflow/data/raw/spotify'
# Получаем ID плейлиста из переменных окружения или используем дефолтный
PLAYLIST_ID = os.getenv('SPOTIFY_PLAYLIST_ID', '18vUeZ9BdtMRNV6gI8RnR6')

# Базовые URL для Spotify API:
# токен для авторизации и endpoint получения треков плейлиста
SPOTIFY_TOKEN_URL = 'https://accounts.spotify.com/api/token'
SPOTIFY_PLAYLIST_URL = 'https://api.spotify.com/v1/playlists/{playlist_id}/tracks'

# Создаём папку для хранения, если её ещё нет
os.makedirs(RAW_PATH, exist_ok=True)


def get_spotify_token():
    """
    Шаг 1: Получение OAuth-токена через client_credentials flow.
    - Берём CLIENT_ID и CLIENT_SECRET из переменных окружения
    - Отправляем POST-запрос на SPOTIFY_TOKEN_URL
    - Проверяем успешность ответа и извлекаем access_token
    - Логируем длину токена для отладки
    """
    client_id = os.getenv('SPOTIFY_CLIENT_ID')
    client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
    # Если креды не заданы — пропускаем задачу без ошибки
    if not client_id or not client_secret:
        raise AirflowSkipException("Spotify credentials not set in environment")

    resp = requests.post(
        SPOTIFY_TOKEN_URL,
        data={'grant_type': 'client_credentials'},
        auth=(client_id, client_secret)
    )
    # Если HTTP-код != 200, вываливаемся с ошибкой
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"[ravelytics] Error obtaining token: {e}")
        raise

    token = resp.json().get('access_token')
    # Если токен не в ответе — пропускаем задачу
    if not token:
        raise AirflowSkipException("Failed to retrieve Spotify token")

    print(f"[ravelytics] Obtained token (len={len(token)})")
    return token


def fetch_playlist_tracks():
    """
    Шаг 2: Скачиваем все треки из плейлиста и сохраняем их в JSON:
    - Получаем токен авторизации
    - Обходим все страницы API (пока есть поле "next")
    - Собираем треки в список all_items
    - Пишем JSON-файл с именем playlist_<ID>_<date>.json
    """
    token = get_spotify_token()
    headers = {'Authorization': f'Bearer {token}'}

    # Начальный URL с подстановкой ID плейлиста
    url = SPOTIFY_PLAYLIST_URL.format(playlist_id=PLAYLIST_ID)
    params = {'limit': 100}  # запрашиваем по 100 треков за раз
    all_items = []

    # Цикл обработки пагинации
    while url:
        resp = requests.get(url, headers=headers, params=params)
        # Если плейлист не найден (HTTP 404) — пропускаем весь DAG run
        if resp.status_code == 404:
            print(f"[ravelytics] Playlist {PLAYLIST_ID} not found (404). Skipping run.")
            raise AirflowSkipException(f"Playlist {PLAYLIST_ID} not found")
        # Проверяем на другие HTTP-ошибки
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"[ravelytics] HTTP error: {e}")
            raise

        data = resp.json()
        items = data.get('items', [])
        print(f"[ravelytics] Fetched {len(items)} items; next={data.get('next')}")
        all_items.extend(items)
        # Переходим к следующей странице
        url = data.get('next')

    # После сбора всех треков формируем имя файла с текущей датой
    today = datetime.datetime.utcnow().strftime('%Y-%m-%d')
    filename = f"playlist_{PLAYLIST_ID}_{today}.json"
    out_path = os.path.join(RAW_PATH, filename)
    print(f"[ravelytics] Writing {len(all_items)} items to {out_path}")

    # Запись списка треков в JSON-файл
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(all_items, f, ensure_ascii=False, indent=2)
    print("[ravelytics] Successfully wrote JSON file")


# ------------------- Описание DAG -------------------
# default_args определяет параметры по умолчанию для всех тасков
default_args = {
    'owner': 'ravelytics',
    'start_date': datetime.datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Создаём сам DAG: идентификатор, расписание (@daily), без догрузки прошлого (catchup=False)
with DAG(
    dag_id='spotify_ingest',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['ravelytics', 'spotify'],
) as dag:

    # Оператор PythonOperator вызывает нашу функцию fetch_playlist_tracks
    task_fetch = PythonOperator(
        task_id='fetch_playlist_tracks',
        python_callable=fetch_playlist_tracks,
    )
