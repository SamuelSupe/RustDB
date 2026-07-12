SELECT o.grp,
       (SELECT 9) AS scalar_value,
       count(CASE WHEN EXISTS (
           SELECT 1
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
       ) THEN 1 END) AS matched
FROM read_csv('__OUTER_DATA__', header = true) AS o
GROUP BY o.grp
ORDER BY o.grp;
