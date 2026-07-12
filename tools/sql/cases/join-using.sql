WITH left_rows AS (
    SELECT id, text AS left_text
    FROM read_csv('__NULL_DATA__', header = true)
    WHERE id <= 4
), right_rows AS (
    SELECT id, text AS right_text
    FROM read_csv('__NULL_DATA__', header = true)
    WHERE id >= 4
)
SELECT id, left_text, right_text
FROM left_rows FULL OUTER JOIN right_rows USING (id)
ORDER BY id;
