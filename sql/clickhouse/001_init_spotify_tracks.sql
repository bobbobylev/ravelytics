-- ClickHouse DDL: ravelytics.spotify_tracks конвейер (Kafka -> MV -> MergeTree)

CREATE DATABASE IF NOT EXISTS ravelytics;

-- Таблица-приёмник
CREATE TABLE IF NOT EXISTS ravelytics.spotify_tracks
(
    ingest_ts    DateTime,
    track_id     String,
    track_name   String,
    artists      Array(String),
    artist_ids   Array(String),
    album_id     String,
    album_name   String,
    release_date Date,
    popularity   UInt8,
    added_at     DateTime,
    external_url String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingest_ts)
ORDER BY (ingest_ts, track_id);

-- Источник Kafka (читаем JSONEachRow из топика)
CREATE TABLE IF NOT EXISTS ravelytics.kafka_spotify_tracks_raw
(
    ingest_ts    String,
    track_id     String,
    track_name   String,
    artists      Array(String),
    artist_ids   Array(String),
    album_id     String,
    album_name   String,
    release_date String,
    popularity   UInt8,
    added_at     String,
    external_url String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list  = 'ravelytics.spotify.tracks',
    kafka_group_name  = 'ch_spotify_tracks',
    kafka_format      = 'JSONEachRow',
    kafka_num_consumers = 1,
    input_format_null_as_default = 1,
    date_time_input_format = 'best_effort';

-- Материализованное представление: стримим из Kafka в MergeTree
CREATE MATERIALIZED VIEW IF NOT EXISTS ravelytics.mv_spotify_tracks
TO ravelytics.spotify_tracks
AS
SELECT
    coalesce(parseDateTimeBestEffortOrNull(ingest_ts), now()) AS ingest_ts,
    track_id,
    track_name,
    artists,
    artist_ids,
    album_id,
    album_name,
    coalesce(
        toDateOrNull(
            if(
                length(release_date) = 4 AND match(release_date, '^[0-9]{4}$'),
                concat(release_date, '-01-01'),
                left(release_date, 10)
            )
        ),
        toDate('1970-01-01')
    ) AS release_date,
    popularity,
    coalesce(parseDateTimeBestEffortOrNull(added_at), ingest_ts) AS added_at,
    external_url
FROM ravelytics.kafka_spotify_tracks_raw;
