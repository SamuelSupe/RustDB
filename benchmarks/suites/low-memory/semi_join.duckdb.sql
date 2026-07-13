SELECT count(*) AS orders_with_lineitems
FROM read_parquet('__TPCH_ROOT__/orders/*.parquet') AS o
WHERE EXISTS (
  SELECT 1
  FROM read_parquet('__TPCH_ROOT__/lineitem/*.parquet') AS l
  WHERE l.l_orderkey = o.o_orderkey
);
