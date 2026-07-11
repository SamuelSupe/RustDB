SELECT x, count(*) AS rows
FROM read_csv('__DATA__', header = true)
GROUP BY x
HAVING rows > 1
ORDER BY x;
