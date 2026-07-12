SELECT EXISTS (
    SELECT 1 / 0
    FROM read_csv('__DATA__', header = true) AS d
    OFFSET 1
) AS has_second_row;
