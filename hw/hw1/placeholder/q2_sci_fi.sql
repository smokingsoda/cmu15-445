SELECT t.primary_title,
    t.premiered,
    CAST(t.runtime_minutes AS VARCHAR) || " (mins)"
FROM titles AS t
WHERE t.genres LIKE "%Sci-Fi%"
ORDER BY t.runtime_minutes DESC
LIMIT 10;