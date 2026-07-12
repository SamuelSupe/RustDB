SELECT o.case_id,
       (SELECT count(*) + 1
        FROM read_csv('__INNER_DATA__', header = true) AS i
        WHERE i.grp = o.grp AND i.key > o.probe) AS count_plus_one,
       (SELECT coalesce(sum(i.key), 42)
        FROM read_csv('__INNER_DATA__', header = true) AS i
        WHERE i.grp = o.grp AND i.key > o.probe) AS sum_default,
       (SELECT count(1)
        FROM read_csv('__INNER_DATA__', header = true) AS i
        WHERE i.grp = o.grp AND i.key > o.probe) AS count_constant,
       (SELECT sum(1)
        FROM read_csv('__INNER_DATA__', header = true) AS i
        WHERE i.grp = o.grp AND i.key > o.probe) AS sum_constant,
       (SELECT sum(coalesce(i.key, 1))
        FROM read_csv('__INNER_DATA__', header = true) AS i
        WHERE i.grp = o.grp AND i.key > o.probe) AS sum_coalesced,
       (SELECT avg(i.key)
        FROM read_csv('__INNER_DATA__', header = true) AS i
        WHERE i.grp = o.grp AND i.key > o.probe) AS filtered_average,
       (SELECT avg(i.key)
        FROM read_csv('__INNER_DATA__', header = true) AS i
        WHERE i.grp = o.grp AND i.key > o.probe
        GROUP BY i.grp) AS grouped_average,
       (SELECT count(*)
        FROM read_csv('__INNER_DATA__', header = true) AS i
        WHERE i.grp = o.grp AND i.key > o.probe
        HAVING count(*) > 0) AS having_count,
       EXISTS (
           SELECT count(*)
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp AND i.key > o.probe
       ) AS aggregate_exists,
       0 IN (
           SELECT count(*)
           FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = o.grp AND i.key > o.probe
       ) AS aggregate_in
FROM read_csv('__OUTER_DATA__', header = true) AS o
ORDER BY o.case_id;
