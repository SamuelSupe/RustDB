SELECT 1 IN (
           SELECT i.key FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = 'match'
       ) AS in_match,
       9 IN (
           SELECT i.key FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = 'miss'
       ) AS in_miss,
       9 IN (
           SELECT i.key FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = 'rhs_null'
       ) AS in_rhs_null,
       CAST(NULL AS BIGINT) IN (
           SELECT i.key FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = 'lhs_null'
       ) AS in_lhs_null,
       CAST(NULL AS BIGINT) IN (
           SELECT i.key FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = 'empty'
       ) AS in_empty,
       1 NOT IN (
           SELECT i.key FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = 'match'
       ) AS not_in_match,
       9 NOT IN (
           SELECT i.key FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = 'miss'
       ) AS not_in_miss,
       9 NOT IN (
           SELECT i.key FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = 'rhs_null'
       ) AS not_in_rhs_null,
       CAST(NULL AS BIGINT) NOT IN (
           SELECT i.key FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = 'lhs_null'
       ) AS not_in_lhs_null,
       CAST(NULL AS BIGINT) NOT IN (
           SELECT i.key FROM read_csv('__INNER_DATA__', header = true) AS i
           WHERE i.grp = 'empty'
       ) AS not_in_empty;
