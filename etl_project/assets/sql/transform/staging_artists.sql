WITH staging_artist AS (
    SELECT
        artist_id,
        LOWER(name) AS artist_name,
        CAST(popularity AS FLOAT) AS popularity,
        genres,
        followers,
        spotify_url, 
        load_date
    FROM
        artists
)

SELECT * FROM staging_artist