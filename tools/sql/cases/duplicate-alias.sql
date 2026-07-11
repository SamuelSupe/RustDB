SELECT x AS z, y AS z
FROM read_csv('__DATA__', header = true)
ORDER BY z, x;
