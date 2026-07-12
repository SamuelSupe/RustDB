SELECT o.grp,
       count(*) IN (SELECT 1) AS aggregate_in,
       sum(o.probe) IN (SELECT 1) AS aggregate_expression_in
FROM read_csv('__OUTER_DATA__', header = true) AS o
GROUP BY o.grp
HAVING count(*) IN (SELECT 1)
ORDER BY o.grp;
