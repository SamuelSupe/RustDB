SELECT val
FROM read_csv('__NULL_DATA__', header = true)
WHERE id <= 6
EXCEPT ALL
SELECT val
FROM read_csv('__NULL_DATA__', header = true)
WHERE id >= 3
ORDER BY val NULLS LAST;
