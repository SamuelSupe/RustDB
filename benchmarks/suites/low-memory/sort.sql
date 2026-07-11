SELECT o_orderkey, o_custkey, o_totalprice
FROM read_parquet('__TPCH_ROOT__/orders/*.parquet') AS orders
ORDER BY o_totalprice DESC, o_orderkey;
