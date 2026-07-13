#!/usr/bin/env python3

import re
import subprocess
import tempfile
import unittest
from pathlib import Path

from provenance import optional_positive_integer, query_metadata


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "benchmarks" / "tpch"
ALL_QUERIES = tuple(f"q{number:02d}" for number in range(1, 23))
SF10_QUERIES = ("q02", "q16", "q17", "q20", "q21", "q22")
TABLES = {
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
}
V03_SQL_CASES = {
    "aggregate-correlated-in",
    "aggregate-empty-uncorrelated-subqueries",
    "aggregate-in-result",
    "aggregate-not-in-null",
    "aggregate-subquery-placement",
    "aggregate-subquery-post",
    "correlated-aggregate-domain",
    "correlated-aggregate-ungrouped-error",
    "correlated-contexts",
    "correlated-dead-distinct-cardinality-runtime-error",
    "correlated-dead-grouped-cardinality-runtime-error",
    "correlated-dead-scalar-cardinality-runtime-error",
    "correlated-exists",
    "correlated-in-null-matrix",
    "correlated-scalar",
    "correlated-scalar-cardinality-runtime-error",
    "distinct-aggregates",
    "exists-aggregate-projection",
    "exists-distinct-offset",
    "exists-offset-projection",
    "exists-projection-not-evaluated",
    "functions-null-numeric",
    "functions-string",
    "functions-temporal",
    "in-null-matrix",
    "null-vs-empty",
    "numeric-coercion",
    "short-circuit",
    "subquery-short-circuit-aggregate",
    "subquery-short-circuit-projection",
    "subquery-short-circuit-shapes",
    "timestamp-microseconds",
    "timestamp-precision",
}
PARQUET_REFERENCE = re.compile(
    r"read_parquet\('__TPCH_ROOT__/([a-z]+)/\*\.parquet'\)", re.IGNORECASE
)


def query_list(name: str) -> tuple[str, ...]:
    lines = (FIXTURES / name).read_text(encoding="utf-8").splitlines()
    return tuple(line.strip() for line in lines if line.strip() and not line.startswith("#"))


class TpchHarnessTests(unittest.TestCase):
    def test_complete_and_targeted_case_lists(self) -> None:
        self.assertEqual(query_list("queries.txt"), ALL_QUERIES)
        self.assertEqual(query_list("cases/sf1-local.txt"), ALL_QUERIES)
        self.assertEqual(query_list("cases/sf1-minio.txt"), ALL_QUERIES)
        self.assertEqual(query_list("cases/sf10-128m.txt"), SF10_QUERIES)

    def test_every_query_is_a_self_contained_parquet_template(self) -> None:
        files = tuple(path.stem for path in sorted(FIXTURES.glob("q[0-9][0-9].sql")))
        self.assertEqual(files, ALL_QUERIES)
        for query in ALL_QUERIES:
            sql = (FIXTURES / f"{query}.sql").read_text(encoding="utf-8")
            self.assertTrue(sql.rstrip().endswith(";"), query)
            tables = set(PARQUET_REFERENCE.findall(sql))
            self.assertTrue(tables, query)
            self.assertLessEqual(tables, TABLES, query)
            self.assertNotIn("__TPCH_ROOT__", PARQUET_REFERENCE.sub("", sql), query)

    def test_feature_queries_keep_the_canonical_substitutions(self) -> None:
        q06 = (FIXTURES / "q06.sql").read_text(encoding="utf-8")
        self.assertIn("BETWEEN 0.05 AND 0.07", q06)
        q16 = (FIXTURES / "q16.sql").read_text(encoding="utf-8")
        self.assertIn("count(DISTINCT ps.ps_suppkey)", q16)
        q17 = (FIXTURES / "q17.sql").read_text(encoding="utf-8")
        self.assertIn("l2.l_partkey = p.p_partkey", q17)
        q21 = (FIXTURES / "q21.sql").read_text(encoding="utf-8")
        self.assertIn("AND EXISTS", q21)
        self.assertIn("AND NOT EXISTS", q21)
        q22 = (FIXTURES / "q22.sql").read_text(encoding="utf-8")
        self.assertIn("substring(c.c_phone FROM 1 FOR 2)", q22)

    def test_compare_help_documents_strict_report_mode_without_setup(self) -> None:
        result = subprocess.run(
            ["sh", str(ROOT / "tools/tpch/compare.sh"), "--help"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("--report", result.stderr)
        self.assertIn("unsupported query still makes the command fail", result.stderr)
        self.assertNotIn("required command not found", result.stderr)

    def test_compare_rejects_an_explicit_empty_rustdb_root_before_setup(self) -> None:
        result = subprocess.run(
            [
                "sh",
                str(ROOT / "tools/tpch/compare.sh"),
                "--rustdb-root",
                "",
                "0.01",
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("--rustdb-root must not be empty", result.stderr)
        self.assertNotIn("required command not found", result.stderr)

    def test_compare_pipes_rendered_sql_into_the_execution_container(self) -> None:
        source = (ROOT / "tools/tpch/compare_query.sh").read_text(encoding="utf-8")
        self.assertIn('set -- "$@" -f /dev/stdin', source)
        self.assertIn('if ! "$@" < "$rustdb_query"', source)
        self.assertNotIn('-f "/workspace/$work_relative/rustdb.sql"', source)

    def test_provenance_hashes_the_query_list_and_every_template(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            fixtures = workspace / "benchmarks/tpch"
            fixtures.mkdir(parents=True)
            (fixtures / "q01.sql").write_text("SELECT 1;\n", encoding="utf-8")
            (fixtures / "q02.sql").write_text("SELECT 2;\n", encoding="utf-8")
            query_list = fixtures / "queries.txt"
            query_list.write_text("q01\nq02\n", encoding="utf-8")

            metadata = query_metadata(workspace, query_list)

            self.assertEqual([entry["id"] for entry in metadata["files"]], ["q01", "q02"])
            self.assertRegex(metadata["list_sha256"], r"^[0-9a-f]{64}$")
            self.assertRegex(metadata["files_sha256"], r"^[0-9a-f]{64}$")
            self.assertNotEqual(
                metadata["list_sha256"], metadata["files_sha256"]
            )

    def test_provenance_optional_positive_integer(self) -> None:
        self.assertIsNone(optional_positive_integer(""))
        self.assertEqual(optional_positive_integer("8192"), 8192)

    def test_v03_sql_differential_contract_is_checked_in(self) -> None:
        cases = ROOT / "tools" / "sql" / "cases"
        names = {path.stem for path in cases.glob("*.sql")}
        self.assertLessEqual(V03_SQL_CASES, names)
        cardinality = cases / "correlated-scalar-cardinality-runtime-error"
        self.assertEqual(
            cardinality.with_suffix(".rustdb-pattern").read_text(encoding="utf-8").strip(),
            "execution error: scalar subquery returned more than one row",
        )
        differential = (ROOT / "tools/sql/differential.sh").read_text(encoding="utf-8")
        for placeholder in ("__NULL_DATA__", "__OUTER_DATA__", "__INNER_DATA__"):
            self.assertIn(placeholder, differential)
        self.assertIn("*-runtime-error", differential)


if __name__ == "__main__":
    unittest.main()
