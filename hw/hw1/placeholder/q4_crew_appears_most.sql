SELECT p.name as name,
    COUNT(*) as nums --must be * because if we use c.characters, then there may be NULL in characters
FROM crew as c,
    people as p
WHERE p.person_id = c.person_id
GROUP BY p.name
ORDER BY nums DESC
LIMIT 20;