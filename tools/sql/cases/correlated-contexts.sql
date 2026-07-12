SELECT o.grp,
       count(*) AS outer_rows
FROM read_csv('__OUTER_DATA__', header = true) AS o
WHERE (EXISTS (
           SELECT *
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
             AND (i.key = o.probe OR i.key IS NULL)
       ) AND o.case_id > 0)
   OR NOT EXISTS (
       SELECT *
       FROM read_csv('__INNER_DATA__', header = true) AS i
       WHERE i.grp = o.grp
   )
GROUP BY o.grp
HAVING (SELECT count(*)
        FROM read_csv('__INNER_DATA__', header = true) AS i
        WHERE i.grp = o.grp) >= 0
ORDER BY o.grp;
