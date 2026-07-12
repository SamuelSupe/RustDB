WITH left_rows AS (
    SELECT x FROM read_csv('__DATA__', header = true)
), right_rows AS (
    SELECT x FROM read_csv('__DATA__', header = true)
)
SELECT left_rows.x AS left_x, right_rows.x AS right_x
FROM left_rows
FULL OUTER JOIN right_rows ON left_rows.x < right_rows.x;
