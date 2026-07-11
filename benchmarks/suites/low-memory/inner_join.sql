SELECT count(*) AS joined_rows
FROM (
  SELECT o_orderkey
  FROM read_parquet('__TPCH_ROOT__/orders/*.parquet')
) AS orders
INNER JOIN (
  SELECT l_orderkey
  FROM read_parquet('__TPCH_ROOT__/lineitem/*.parquet')
) AS l
  ON orders.o_orderkey = l.l_orderkey;
