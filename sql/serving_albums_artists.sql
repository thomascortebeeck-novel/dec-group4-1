WITH joined_data AS (
    SELECT
        a.artist_id,
        a.artist_name,
        a.popularity,
        a.genres,
        a.followers,
        a.spotify_url,
        p.playlist_country_lower
    FROM
        artist a
    JOIN
        playlists p ON a.artist_id = b.artist_id
)
-- how to reference these staging tables? 
SELECT 
    *, 
    SUM(followers) OVER (PARTITION BY playlist_country_lower) AS total_followers_per_country
FROM joined_data;