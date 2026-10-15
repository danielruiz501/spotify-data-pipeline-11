-- 1. View processed tracks
SELECT *
FROM spotify.tracks
ORDER BY popularity DESC
LIMIT 10;

-- 2. Count tracks by source
SELECT
    source,
    COUNT(*) AS total_tracks
FROM spotify.tracks
GROUP BY source
ORDER BY total_tracks DESC;

-- 3. Calculate average popularity by source
SELECT
    source,
    COUNT(*) AS total_tracks,
    ROUND(AVG(popularity), 2) AS avg_popularity
FROM spotify.tracks
GROUP BY source
ORDER BY total_tracks DESC;

-- 4. Calculate overall track statistics
SELECT
    COUNT(*) AS total_tracks,
    ROUND(AVG(popularity), 2) AS average_popularity,
    MAX(popularity) AS highest_popularity,
    MIN(popularity) AS lowest_popularity
FROM spotify.tracks;