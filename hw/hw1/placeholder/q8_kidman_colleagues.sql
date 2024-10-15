SELECT DISTINCT(p.name)
FROM people as p,
    crew as c,
    (
        SELECT c.title_id as has_the_guy
        FROM crew as c,
            people as p
        WHERE p.person_id = c.person_id
            AND p.name = "Nicole Kidman"
            AND p.born = 1967
    )
WHERE (
        c.category = "actor"
        OR c.category = "actress"
    )
    AND has_the_guy = c.title_id
    AND c.person_id = p.person_id
ORDER BY p.name ASC;
