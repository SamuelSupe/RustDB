WITH left_rows AS (
    SELECT val FROM read_csv('__NULL_DATA__', header = true) WHERE grp = 'a'
), right_rows AS (
    SELECT val FROM read_csv('__NULL_DATA__', header = true) WHERE grp = 'b'
)
SELECT val FROM left_rows
UNION
SELECT val FROM right_rows
ORDER BY val NULLS LAST;
