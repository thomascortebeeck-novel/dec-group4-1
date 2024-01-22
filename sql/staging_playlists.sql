WITH staging_playlists AS (
    SELECT
        artist_id,
        LOWER(playlist_country) AS playlist_country_lower
    FROM
        playlists
)

-- Use the CTE in your further queries if needed
SELECT *
FROM staging_playlists;