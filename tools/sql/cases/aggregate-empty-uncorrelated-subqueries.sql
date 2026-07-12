SELECT count(*) AS n,
       (SELECT count(*)
        FROM read_csv('__NULL_DATA__', header = true)) AS scalar_value,
       EXISTS (SELECT 1) AS exists_value,
       7 IN (SELECT count(*)
             FROM read_csv('__NULL_DATA__', header = true)) AS in_value,
       CASE WHEN EXISTS (SELECT 1) THEN count(*) ELSE 99 END AS case_value
FROM read_csv('__NULL_DATA__', header = true) AS o
WHERE o.id < 0
HAVING (SELECT count(*)
        FROM read_csv('__NULL_DATA__', header = true)) = 7;
