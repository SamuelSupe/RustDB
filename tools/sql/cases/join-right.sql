WITH left_rows AS (
    SELECT id, val FROM read_csv('__NULL_DATA__', header = true) WHERE id <= 4
), right_rows AS (
    SELECT id, val FROM read_csv('__NULL_DATA__', header = true) WHERE id >= 3
)
SELECT left_rows.id AS left_id, right_rows.id AS right_id,
       left_rows.val AS left_val, right_rows.val AS right_val
FROM left_rows
RIGHT JOIN right_rows
  ON left_rows.id = right_rows.id AND left_rows.val = right_rows.val
ORDER BY right_id;
