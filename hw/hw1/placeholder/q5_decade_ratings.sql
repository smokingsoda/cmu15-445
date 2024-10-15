SELECT CAST(FLOOR(t.premiered / 10) * 10 AS VARCHAR) || "s" AS decades,
    ROUND(AVG(r.rating), 2) AS avg_rating,
    MAX(r.rating) AS max_rating,
    MIN(r.rating) AS min_rating,
    COUNT(r.title_id) AS rating_count
FROM titles AS t
    JOIN ratings AS r ON t.title_id = r.title_id
WHERE decades IS NOT NULL
GROUP BY decades
ORDER BY avg_rating DESC, decades ASC;