SELECT nullif(CAST(NULL AS BIGINT), (
           SELECT i.key
           FROM read_csv('__INNER_DATA__', header = true) AS i
       )) AS dead_nullif,
       CASE WHEN false THEN 1 IN (SELECT 1 / 0) ELSE false END AS dead_in,
       CASE WHEN false THEN (SELECT 1 / 0 ORDER BY 1 LIMIT 1) ELSE 0 END AS dead_ordered;
