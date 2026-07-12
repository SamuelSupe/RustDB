SELECT CASE WHEN false THEN (
           SELECT max(i.key)
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
           GROUP BY i.key
       ) ELSE 0 END AS dead_grouped
FROM read_csv('__OUTER_DATA__', header = true) AS o
WHERE o.grp = 'multi';
