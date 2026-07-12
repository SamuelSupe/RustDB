SELECT o.case_id,
       CASE WHEN false THEN (
           SELECT i.key
           FROM read_csv('__INNER_DATA__', header = true) AS i
       ) ELSE 0 END AS dead_case,
       false AND ((
           SELECT i.key
           FROM read_csv('__INNER_DATA__', header = true) AS i
       ) = 1) AS dead_and,
       true OR ((
           SELECT i.key
           FROM read_csv('__INNER_DATA__', header = true) AS i
       ) = 1) AS dead_or,
       false AND ((
           SELECT i.key
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
       ) = 1) AS correlated_dead_and
FROM read_csv('__OUTER_DATA__', header = true) AS o
ORDER BY o.case_id;
