SELECT *, count(*) AS rows
FROM (
  SELECT x
  FROM read_csv('__DATA__', header = true)
) AS one_column
GROUP BY 1
ORDER BY 1;
