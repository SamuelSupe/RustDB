SELECT o.case_id,
       EXISTS (
           SELECT *
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
       ) AS group_exists,
       NOT EXISTS (
           SELECT *
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
       ) AS group_not_exists,
       EXISTS (
           SELECT *
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
             AND i.key <> o.probe
       ) AS different_key_exists,
       NOT EXISTS (
           SELECT *
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
             AND i.key <> o.probe
       ) AS different_key_not_exists
FROM read_csv('__OUTER_DATA__', header = true) AS o
ORDER BY o.case_id;
