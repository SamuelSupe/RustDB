SELECT id,
       grp,
       val,
       ntile(2) OVER w AS tile,
       percent_rank() OVER w AS relative_rank,
       cume_dist() OVER w AS cumulative_distribution
FROM read_csv('__NULL_DATA__', header = true)
WINDOW w AS (PARTITION BY grp ORDER BY val NULLS LAST)
ORDER BY grp, val NULLS LAST, id;
