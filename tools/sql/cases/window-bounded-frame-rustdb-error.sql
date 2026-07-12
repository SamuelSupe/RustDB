SELECT sum(x) OVER (
           ORDER BY x
           ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
       ) AS bounded_sum
FROM read_csv('__DATA__', header = true);
