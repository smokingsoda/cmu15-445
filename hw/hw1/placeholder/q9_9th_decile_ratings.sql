WITH cte1 (rNames, rTitles) AS (
    SELECT p.name,
        t.title_id
    FROM titles AS t
        INNER JOIN crew AS c ON t.title_id = c.title_id
        INNER JOIN people AS p ON p.person_id = c.person_id
    WHERE p.born = 1955
        AND t.type = "movie"
),
cte2 (rNames, rRatings) AS (
    SELECT c1.rNames,
        ROUND(AVG(r.rating), 2) AS res
    FROM cte1 AS c1
        INNER JOIN ratings AS r ON c1.rTitles = r.title_id
    GROUP BY c1.rNames
),
cte3 (n, r, b) AS (
    SELECT c2.rNames,
        c2.rRatings,
        NTILE(10) OVER (
            ORDER BY c2.rRatings ASC
        ) AS buckets
    FROM cte2 AS c2
)
SELECT n,
    r
FROM cte3
WHERE b = 9
ORDER BY r DESC, n ASC;