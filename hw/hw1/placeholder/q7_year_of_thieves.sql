SELECT COUNT(t.title_id)
FROM titles as t
WHERE t.premiered IN (
        SELECT premiered
        FROM titles
        WHERE primary_title = "Army of Thieves"
        LIMIT 1
    );