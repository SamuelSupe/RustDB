SELECT cntrycode,
       count(*) AS numcust,
       sum(c_acctbal) AS totacctbal
FROM (
    SELECT substring(c.c_phone FROM 1 FOR 2) AS cntrycode,
           c.c_acctbal
    FROM read_parquet('__TPCH_ROOT__/customer/*.parquet') AS c
    WHERE substring(c.c_phone FROM 1 FOR 2)
          IN ('13', '31', '23', '29', '30', '18', '17')
      AND c.c_acctbal > (
          SELECT avg(c2.c_acctbal)
          FROM read_parquet('__TPCH_ROOT__/customer/*.parquet') AS c2
          WHERE c2.c_acctbal > 0.00
            AND substring(c2.c_phone FROM 1 FOR 2)
                IN ('13', '31', '23', '29', '30', '18', '17')
      )
      AND NOT EXISTS (
          SELECT *
          FROM read_parquet('__TPCH_ROOT__/orders/*.parquet') AS o
          WHERE o.o_custkey = c.c_custkey
      )
) AS custsale
GROUP BY cntrycode
ORDER BY cntrycode;
