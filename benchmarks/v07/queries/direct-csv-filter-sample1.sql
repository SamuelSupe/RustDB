SELECT sum(id) AS total
FROM read_csv(
    '/workspace/data/csv-scaling-smoke.csv',
    header = true,
    sample_size = 1
)
WHERE id > 0;
