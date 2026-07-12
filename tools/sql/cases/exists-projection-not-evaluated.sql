SELECT o.case_id,
       EXISTS (
           SELECT 1 / 0
           FROM read_csv('__INNER_DATA__', header = true) AS i
       ) AS plain_exists,
       EXISTS (
           SELECT 1 / 0
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
       ) AS correlated_exists,
       NOT EXISTS (
           SELECT 1 / 0
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp AND i.key > 100
       ) AS correlated_not_exists,
       EXISTS (
           SELECT DISTINCT 1 / 0
           FROM read_csv('__INNER_DATA__', header = true) AS i
       ) AS distinct_exists,
       EXISTS (
           SELECT DISTINCT 1 / 0
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
       ) AS correlated_distinct_exists,
       CASE WHEN EXISTS (SELECT 1 / 0) THEN 7 ELSE 0 END AS case_value
FROM read_csv('__OUTER_DATA__', header = true) AS o
ORDER BY o.case_id;
