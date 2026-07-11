SELECT x AS label, count(*) AS rows
FROM read_csv('__DATA__', header = true)
GROUP BY 1
ORDER BY 1;
