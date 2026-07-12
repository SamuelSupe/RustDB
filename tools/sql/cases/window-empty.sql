SELECT count(*) AS output_rows,
       max(row_number) AS max_row_number,
       max(rows_in_input) AS max_rows_in_input
FROM (
    SELECT row_number() OVER (ORDER BY x) AS row_number,
           count(*) OVER () AS rows_in_input
    FROM read_csv('__DATA__', header = true)
    WHERE false
) AS empty_window;
