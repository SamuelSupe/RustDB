SELECT o.case_id,
       (SELECT i.key
        FROM read_csv('__INNER_DATA__', header = true) AS i
        WHERE i.grp = o.grp) AS one_key
FROM read_csv('__OUTER_DATA__', header = true) AS o
WHERE o.case_id = 6;
