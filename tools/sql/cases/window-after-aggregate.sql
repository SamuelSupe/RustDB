SELECT x,
       count(*) AS rows,
       rank() OVER (ORDER BY count(*) DESC, x) AS frequency_rank
FROM read_csv('__DATA__', header = true)
GROUP BY x
QUALIFY frequency_rank <= 2
ORDER BY x;
