SELECT o.case_id,
       EXISTS (
           SELECT sum(1 / 0)
           FROM read_csv('__INNER_DATA__', header = true) AS i
       ) AS aggregate_exists,
       EXISTS (
           SELECT sum(1 / 0)
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
       ) AS correlated_aggregate_exists,
       EXISTS (
           SELECT sum(1 / 0)
           FROM read_csv('__INNER_DATA__', header = true) AS i
           HAVING count(*) > 0
       ) AS having_exists,
       CASE WHEN EXISTS (
           SELECT sum(1 / 0)
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
           HAVING count(*) > 0
       ) THEN 7 ELSE 0 END AS aggregate_case_value
FROM read_csv('__OUTER_DATA__', header = true) AS o
ORDER BY o.case_id;
