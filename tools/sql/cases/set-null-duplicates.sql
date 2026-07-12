WITH left_rows AS (
    SELECT val
    FROM read_csv('__NULL_DATA__', header = true)
    WHERE id <= 3
), right_rows AS (
    SELECT val
    FROM read_csv('__NULL_DATA__', header = true)
    WHERE id = 2 OR id = 3
)
SELECT 'union' AS operation, val
FROM (SELECT val FROM left_rows UNION SELECT val FROM right_rows) AS union_rows
UNION ALL
SELECT 'intersect' AS operation, val
FROM (SELECT val FROM left_rows INTERSECT SELECT val FROM right_rows) AS intersect_rows
UNION ALL
SELECT 'except' AS operation, val
FROM (SELECT val FROM left_rows EXCEPT SELECT val FROM right_rows) AS except_rows
ORDER BY operation, val NULLS LAST;
