SELECT val
FROM read_csv('__NULL_DATA__', header = true)
WHERE id <= 6
INTERSECT ALL
SELECT val
FROM read_csv('__NULL_DATA__', header = true)
WHERE id >= 2
ORDER BY val NULLS LAST;
