SELECT ntile(0) OVER (ORDER BY x) AS tile
FROM read_csv('__DATA__', header = true);
