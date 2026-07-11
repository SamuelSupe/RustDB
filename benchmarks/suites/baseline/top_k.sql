SELECT o_orderkey, o_totalprice, o_orderdate
FROM read_parquet('__TPCH_ROOT__/orders/*.parquet') AS orders
ORDER BY o_totalprice DESC, o_orderkey
LIMIT 1000;
