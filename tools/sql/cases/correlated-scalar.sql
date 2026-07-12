SELECT o.case_id,
       (SELECT max(i.key)
        FROM read_csv('__INNER_DATA__', header = true) AS i
        WHERE i.grp = o.grp) AS maximum_key,
       (SELECT count(*)
        FROM read_csv('__INNER_DATA__', header = true) AS i
        WHERE i.grp = o.grp) AS key_count,
       CASE WHEN (SELECT max(i.key)
                  FROM read_csv('__INNER_DATA__', header = true) AS i
                  WHERE i.grp = o.grp) IS NULL
            THEN 'none' ELSE 'some' END AS key_class
FROM read_csv('__OUTER_DATA__', header = true) AS o
ORDER BY o.case_id;
