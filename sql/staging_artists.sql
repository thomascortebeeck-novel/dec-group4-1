WITH staging_artist AS (
    SELECT
        artist_id,
        LOWER(name) AS artist_name,
        CAST(popularity AS FLOAT) AS popularity,
        REPLACE(REPLACE(REPLACE(genres, '"', ''), '{', ''), '}', '') AS genres,
        followers,
        spotify_url
    FROM
        artist
)

SELECT * FROM staging_artist;