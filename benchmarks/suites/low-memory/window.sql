SELECT count(*) AS retained_rows,
       sum(row_in_status) AS row_number_checksum
FROM (
  SELECT row_number() OVER (
           PARTITION BY o_orderstatus
           ORDER BY o_totalprice DESC, o_orderkey
         ) AS row_in_status
  FROM read_parquet('__TPCH_ROOT__/orders/*.parquet')
  QUALIFY row_in_status <= 100
) AS ranked_orders;
