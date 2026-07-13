SELECT count(*) AS customers_without_orders
FROM read_parquet('__TPCH_ROOT__/customer/*.parquet') AS c
WHERE NOT EXISTS (
  SELECT 1
  FROM read_parquet('__TPCH_ROOT__/orders/*.parquet') AS o
  WHERE o.o_custkey = c.c_custkey
);
