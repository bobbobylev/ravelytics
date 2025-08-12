-- Техно-моделирование: артисты, жанры, флаг is_techno

DROP VIEW IF EXISTS ravelytics.v_track_is_techno;
DROP VIEW IF EXISTS ravelytics.v_track_genres;
DROP VIEW IF EXISTS ravelytics.v_track_artists;
DROP VIEW IF EXISTS ravelytics.v_artist_latest;

-- Последнее состояние артиста
CREATE VIEW ravelytics.v_artist_latest AS
SELECT
    artist_id,
    argMax(artist_name, ingest_ts) AS artist_name,
    argMax(genres,      ingest_ts) AS genres
FROM ravelytics.spotify_artists
GROUP BY artist_id;

-- По одному артисту в строке для каждого трека
CREATE VIEW ravelytics.v_track_artists AS
SELECT
    ingest_ts,
    track_id,
    track_name,
    arrayJoin(artist_ids) AS artist_id
FROM ravelytics.spotify_tracks;

-- Жанры трека (агрегация жанров всех артистов)
CREATE VIEW ravelytics.v_track_genres AS
SELECT
    ta.ingest_ts,
    ta.track_id,
    ta.track_name,
    arrayDistinct(
        arrayMap(g -> lowerUTF8(g),
            arrayFlatten(
                groupArray(
                    if(isNull(al.artist_id), emptyArrayString(), al.genres)
                )
            )
        )
    ) AS all_genres
FROM ravelytics.v_track_artists AS ta
LEFT JOIN ravelytics.v_artist_latest AS al
    ON ta.artist_id = al.artist_id
GROUP BY ta.ingest_ts, ta.track_id, ta.track_name;

-- Флаг "техно"
CREATE VIEW ravelytics.v_track_is_techno AS
WITH
    arrayMap(g -> lowerUTF8(g), all_genres) AS gl,
    arrayFilter(g -> positionCaseInsensitive(g, 'techno') > 0, gl) AS matched
SELECT
    ingest_ts,
    track_id,
    track_name,
    all_genres,
    matched,
    length(matched) > 0 AS is_techno
FROM ravelytics.v_track_genres;
