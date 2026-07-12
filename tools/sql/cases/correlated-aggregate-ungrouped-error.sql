SELECT CASE WHEN EXISTS (
           SELECT 1
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
       ) THEN count(*) ELSE 0 END
FROM read_csv('__OUTER_DATA__', header = true) AS o;
