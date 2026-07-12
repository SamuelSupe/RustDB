SELECT o.case_id,
       o.probe IN (
           SELECT i.key
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
       ) AS in_result,
       o.probe NOT IN (
           SELECT i.key
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp
       ) AS not_in_result
FROM read_csv('__OUTER_DATA__', header = true) AS o
ORDER BY o.case_id;
