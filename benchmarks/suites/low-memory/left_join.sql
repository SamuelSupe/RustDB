SELECT count(*) AS output_rows,
       count(o.o_orderkey) AS matched_orders
FROM (
  SELECT c_custkey
  FROM read_parquet('__TPCH_ROOT__/customer/*.parquet')
) AS c
LEFT JOIN (
  SELECT o_orderkey, o_custkey
  FROM read_parquet('__TPCH_ROOT__/orders/*.parquet')
) AS o
  ON c.c_custkey = o.o_custkey;
