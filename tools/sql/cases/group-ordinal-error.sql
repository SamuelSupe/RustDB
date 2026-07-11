SELECT x, count(*)
FROM read_csv('__DATA__', header = true)
GROUP BY 3;
