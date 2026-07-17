SELECT count(id) AS row_count
FROM read_csv('/workspace/data/csv-scaling-smoke.csv', header = true);
