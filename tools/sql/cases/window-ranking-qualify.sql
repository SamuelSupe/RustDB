SELECT x,
       y,
       row_number() OVER w AS row_no,
       rank() OVER w AS peer_rank,
       dense_rank() OVER w AS dense_peer_rank,
       count(*) OVER (PARTITION BY x) AS partition_rows
FROM read_csv('__DATA__', header = true)
WINDOW w AS (PARTITION BY x ORDER BY y)
QUALIFY row_no <= 2
ORDER BY x, y;
