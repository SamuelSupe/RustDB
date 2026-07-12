WITH left_rows AS (
    SELECT id, val FROM read_csv('__NULL_DATA__', header = true) WHERE id <= 5
), right_rows AS (
    SELECT id, val + 1 AS val FROM read_csv('__NULL_DATA__', header = true) WHERE id >= 4
)
SELECT coalesce(left_rows.id, right_rows.id) AS id,
       left_rows.val AS left_val,
       right_rows.val AS right_val
FROM left_rows
FULL OUTER JOIN right_rows
  ON left_rows.id = right_rows.id AND left_rows.val < right_rows.val
ORDER BY id, left_val NULLS LAST, right_val NULLS LAST;
