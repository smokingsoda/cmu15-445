SELECT name, 
CASE
    WHEN died IS NULL
    THEN 2022 - born
    ELSE died - born
    END AS age
FROM people
WHERE born >= 1900
ORDER BY age DESC, name
LIMIT 20;