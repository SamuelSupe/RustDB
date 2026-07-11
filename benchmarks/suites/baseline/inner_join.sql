SELECT count(*) AS joined_rows,
       sum(l.l_extendedprice) AS extended_price
FROM (
  SELECT o_orderkey
  FROM read_parquet('__TPCH_ROOT__/orders/*.parquet')
) AS o
INNER JOIN (
  SELECT l_orderkey, l_extendedprice
  FROM read_parquet('__TPCH_ROOT__/lineitem/*.parquet')
) AS l
  ON o.o_orderkey = l.l_orderkey;
