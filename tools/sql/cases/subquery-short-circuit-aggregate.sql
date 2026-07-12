SELECT o.case_id,
       CASE WHEN false THEN (
           SELECT min(i.key) / 0
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
       ) ELSE 0 END AS dead_aggregate
FROM read_csv('__OUTER_DATA__', header = true) AS o
ORDER BY o.case_id;
