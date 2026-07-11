SELECT y AS value
FROM read_csv('__DATA__', header = true)
ORDER BY CASE WHEN y = 'b' THEN NULL ELSE y END DESC;
