WITH left_rows AS (
    SELECT CASE WHEN y = 'b' THEN NULL ELSE x END AS key, y AS left_value
    FROM read_csv('__DATA__', header = true)
), right_rows AS (
    SELECT CASE WHEN y = 'c' THEN NULL ELSE x END AS key, y AS right_value
    FROM read_csv('__DATA__', header = true)
)
SELECT coalesce(left_rows.key, right_rows.key) AS key,
       left_rows.left_value,
       right_rows.right_value
FROM left_rows
FULL OUTER JOIN right_rows ON left_rows.key = right_rows.key
ORDER BY key NULLS LAST, left_value NULLS LAST, right_value NULLS LAST;
