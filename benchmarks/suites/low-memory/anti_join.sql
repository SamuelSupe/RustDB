SELECT count(*) AS customers_without_orders
FROM (
  SELECT c_custkey
  FROM read_parquet('__TPCH_ROOT__/customer/*.parquet')
) AS c
LEFT ANTI JOIN (
  SELECT o_custkey
  FROM read_parquet('__TPCH_ROOT__/orders/*.parquet')
) AS o
  ON c.c_custkey = o.o_custkey;
