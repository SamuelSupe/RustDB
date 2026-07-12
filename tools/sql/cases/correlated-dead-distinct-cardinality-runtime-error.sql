SELECT CASE WHEN false THEN (
           SELECT DISTINCT i.key
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
       ) ELSE 0 END AS dead_distinct
FROM read_csv('__OUTER_DATA__', header = true) AS o
WHERE o.grp = 'multi';
