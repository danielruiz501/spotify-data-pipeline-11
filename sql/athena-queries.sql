-- 1. View processed tracks
SELECT *
FROM spotify_data_lake_db.processed_tracks
LIMIT 10;

-- 2. Count tracks by source
SELECT
    source,
    COUNT(*) AS total_tracks
FROM spotify_data_lake_db.processed_tracks
GROUP BY source
ORDER BY total_tracks DESC;

-- 3. Calculate average popularity by source
SELECT
    source,
    COUNT(*) AS total_tracks,
    ROUND(AVG(popularity), 2) AS avg_popularity
FROM spotify_data_lake_db.processed_tracks
GROUP BY source
ORDER BY total_tracks DESC;