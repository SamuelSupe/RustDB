SELECT o.grp,
       count(*) IN (
           SELECT i.key
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
       ) AS correlated_aggregate_in
FROM read_csv('__OUTER_DATA__', header = true) AS o
GROUP BY o.grp
HAVING count(*) IN (
    SELECT i.key
    FROM read_csv('__INNER_DATA__', header = true) AS i
    WHERE i.grp = o.grp
)
ORDER BY o.grp;
