SELECT count(*) AS matched_rows,
       cast(sum(l.c_custkey) AS BIGINT) AS left_key_sum
FROM read_parquet('data/tpch-sf1/customer/*.parquet') AS l
JOIN read_parquet('data/tpch-sf1/customer/*.parquet') AS r
  ON l.c_name = r.c_name;
