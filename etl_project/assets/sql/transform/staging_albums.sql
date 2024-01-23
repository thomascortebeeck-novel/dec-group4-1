WITH staging_albums AS (
    SELECT
        album_id,
        LOWER(album_name) AS album_name,
        album_type,
        release_date,
        total_tracks,
        artist_id,
        load_date
    FROM
        albums
)

SELECT * FROM staging_albums;