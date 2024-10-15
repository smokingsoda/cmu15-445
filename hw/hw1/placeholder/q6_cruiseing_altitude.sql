SELECT t.primary_title,
    r.votes
FROM titles as t,
    ratings as r,
    crew as c,
    people as p
WHERE t.title_id = r.title_id
    AND t.title_id = c.title_id
    AND p.born = 1962
    AND p.name LIKE "%Cruise%"
    AND c.person_id = p.person_id
ORDER BY r.votes DESC
LIMIT 10