SELECT EXISTS (
    SELECT DISTINCT d.x
    FROM read_csv('__DATA__', header = true) AS d
    WHERE d.x = 1
    OFFSET 1
) AS has_second_distinct_group;
