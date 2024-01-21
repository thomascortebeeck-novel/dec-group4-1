WITH joined_data AS (
    SELECT
        a.artist_id,
        a.name,
        a.popularity,
        a.genres,
        a.followers,
        a.spotify_url,
        p.playlist_country as playlist_country, 
	    SUM(a.followers) OVER (PARTITION BY p.playlist_country) AS country_total_followers,
        AVG(a.popularity) OVER (PARTITION BY p.playlist_country, a.artist_id) AS avg_popularity_per_artist_per_country,
		RANK() OVER (PARTITION BY p.playlist_country ORDER BY a.popularity DESC) AS popularity_rank
    FROM
        artists a
    JOIN
        playlist p ON a.artist_id = p.artist_id
)
-- how to reference these staging tables? 
SELECT 
    *
FROM joined_data