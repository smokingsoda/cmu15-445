WITH cte1 (tRes, i) AS (
    SELECT title,
        ROW_NUMBER() OVER (
            ORDER BY title ASC
        ) AS i
    FROM akas
        JOIN (
            SELECT title_id AS res
            FROM titles
            WHERE primary_title = "House of the Dragon"
                AND type = "tvSeries"
        ) ON res = title_id
    WHERE is_original_title = 0
    GROUP BY title
    ORDER BY title ASC
),
cte2 (result, count) AS (
    SELECT tRes,
        1
    FROM cte1
    WHERE i = 1
    UNION ALL
    SELECT c2.result || ", " || c1.tRes,
        c2.count + 1
    FROM cte1 AS c1
        JOIN cte2 AS c2 ON c1.i = c2.count + 1
)
SELECT result
FROM cte2
ORDER by count DESC
LIMIT 1;