SELECT sum(id) AS total
FROM read_csv('/workspace/data/csv-scaling-smoke.csv', header = true)
WHERE id > 0;
