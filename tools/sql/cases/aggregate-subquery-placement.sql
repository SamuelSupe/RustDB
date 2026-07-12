SELECT sum((SELECT 2)) AS scalar_sum,
       count(CASE WHEN EXISTS (SELECT 1) THEN 1 END) AS exists_count
FROM read_csv('__OUTER_DATA__', header = true) AS o;
