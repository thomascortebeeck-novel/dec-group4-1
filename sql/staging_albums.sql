WITH staging_albums AS (
    SELECT
        album_id,
        LOWER(name) AS album_name,
        release_date
        total_tracks,
        artist_id
    FROM
        albums
)

SELECT * FROM staging_albums;