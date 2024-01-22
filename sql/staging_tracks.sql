WITH staging_tracks AS (
    SELECT
        track_id,
        LOWER(name) AS track_name,
        ROUND(duration_ms / 60000.0, 2) AS duration_minutes,
        CAST(popularity AS FLOAT) AS popularity,
        explicit,
        preview_url,
        track_number,
        artist_id,
        album_id
    FROM
        tracks
    WHERE
        explicit = true
)

SELECT * FROM staging_tracks;
