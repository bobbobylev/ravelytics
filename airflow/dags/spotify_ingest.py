# Импорт необходимых модулей:
# os, json, datetime для работы с файловой системой, сериализации и времени
# requests для HTTP-запросов к API Spotify
# DAG и PythonOperator из Airflow для описания и выполнения задач
# AirflowSkipException для корректного пропуска задач при отсутствии данных
import os
import json
import datetime
import requests
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
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

def preflight_check():
    playlist_id = Variable.get("SPOTIFY_PLAYLIST_ID", default_var=os.getenv("SPOTIFY_PLAYLIST_ID", ""))
    if not playlist_id:
        raise AirflowSkipException("SPOTIFY_PLAYLIST_ID is empty")

    token = get_spotify_token()  # твоя функция получения токена
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    r = requests.get(url, headers=headers, params={"limit": 1}, timeout=20)

    if r.status_code in (403, 404):
        # мягко выходим — даг помечается как SKIPPED
        raise AirflowSkipException(f"Playlist {playlist_id} not accessible ({r.status_code})")
    r.raise_for_status()

def fetch_playlist_tracks():
    """
    Тянем треки публичного плейлиста и сохраняем items в RAW_JSON_DIR.
    - playlist_id читаем динамически: Airflow Variable -> .env
    - 403 (приватный плейлист) -> помечаем таск как SKIPPED, а не FAIL
    - пагинация по next
    """
    playlist_id = Variable.get(
        "SPOTIFY_PLAYLIST_ID",
        default_var=os.getenv("SPOTIFY_PLAYLIST_ID", "")
    )
    if not playlist_id:
        raise AirflowSkipException("SPOTIFY_PLAYLIST_ID is empty")

    # путь для сохранения
    raw_dir = os.getenv("RAW_JSON_DIR", "/opt/airflow/data/raw/spotify")
    os.makedirs(raw_dir, exist_ok=True)

    # токен и заголовки
    token = get_spotify_token()  # твоя функция получения client-credentials токена
    headers = {"Authorization": f"Bearer {token}"}

    # сбор всех items с пагинацией
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    params = {"limit": 100}
    items = []

    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=20)
        if resp.status_code == 403:
            # плейлист приватный/недоступен для client-credentials
            raise AirflowSkipException(f"Playlist {playlist_id} is private/inaccessible (403)")
        resp.raise_for_status()

        data = resp.json()
        batch = data.get("items", [])
        items.extend(batch)

        next_url = data.get("next")
        if not next_url:
            break
        # next уже содержит query-параметры, поэтому дальше идём без params
        url = next_url
        params = {}

    if not items:
        raise AirflowSkipException("Playlist returned no items")

    # сохраняем в файл
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(raw_dir, f"playlist_{playlist_id}_{ts}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False)

    print(f"[ravelytics] Saved {len(items)} items -> {out_path}")



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

    preflight = PythonOperator(task_id="preflight_check", python_callable=preflight_check)
    fetch    = PythonOperator(task_id="fetch_playlist_tracks", python_callable=fetch_playlist_tracks)
    preflight >> fetch
