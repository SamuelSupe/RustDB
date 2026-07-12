WITH left_rows AS (
    SELECT val FROM read_csv('__NULL_DATA__', header = true) WHERE id <= 4
), right_rows AS (
    SELECT val FROM read_csv('__NULL_DATA__', header = true) WHERE id = 2 OR id = 4
)
SELECT val FROM left_rows
EXCEPT
SELECT val FROM right_rows
ORDER BY val NULLS LAST;
