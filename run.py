"""Qafila gross margin pipeline: raw files -> DuckDB -> dbt -> Iceberg.

    uv run python run.py --data-dir data --warehouse warehouse

The pipeline must work on a fresh warehouse and stay safe to re-run with the same input.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import duckdb
from pyiceberg.catalog.sql import SqlCatalog

DBT_DIR = Path(__file__).resolve().parent / "dbt"


def get_catalog(warehouse: Path) -> SqlCatalog:
    """Return the Iceberg catalog used by the project grader."""
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        "qafila",
        uri=f"sqlite:///{(warehouse / 'catalog.db').as_posix()}",
        warehouse=warehouse.resolve().as_posix(),
    )


def ingest(data_dir: Path, db: Path) -> None:
    """Load the OLTP tables and every supplier price CSV into schema raw."""
    sqlite_file = (data_dir / "qafila_oltp.sqlite").resolve()
    price_files = sorted((data_dir / "suppliers").glob("prices_*.csv"))

    if not sqlite_file.exists():
        raise FileNotFoundError(f"Missing {sqlite_file}")
    if not price_files:
        raise FileNotFoundError(f"No prices_*.csv files in {data_dir / 'suppliers'}")

    with duckdb.connect(str(db)) as con:
        con.sql("CREATE SCHEMA IF NOT EXISTS raw")

        for table in ("orders", "order_items", "products", "stores"):
            con.execute(
                f"""
                CREATE OR REPLACE TABLE raw.{table} AS
                SELECT *
                FROM sqlite_scan('{sqlite_file.as_posix()}', '{table}')
                """
            )

        con.execute(
            """
            CREATE OR REPLACE TABLE raw.supplier_prices AS
            SELECT *
            FROM read_csv_auto(
                ?,
                all_varchar = true,
                union_by_name = true,
                filename = true
            )
            """,
            [[str(path.resolve()) for path in price_files]],
        )


def transform(db: Path) -> None:
    """Run dbt build in a separate process so DuckDB is released before publishing."""
    env = {**os.environ, "QAFILA_DUCKDB": str(db.resolve())}
    command = [
        sys.executable,
        "-c",
        "from dbt.cli.main import cli; cli()",
        "build",
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
    ]
    if subprocess.run(command, env=env).returncode != 0:
        raise SystemExit("dbt build failed")


def _same_rows(left, right) -> bool:
    """Compare two Arrow tables without depending on row order."""
    if left.num_rows != right.num_rows or set(left.column_names) != set(right.column_names):
        return False
    left_rows = sorted(repr(row) for row in left.to_pylist())
    right_rows = sorted(repr(row) for row in right.to_pylist())
    return left_rows == right_rows


def publish(db: Path, warehouse: Path) -> None:
    """Publish dbt outputs to Iceberg without creating snapshots for unchanged data."""
    catalog = get_catalog(warehouse)
    outputs = (
        ("silver", "product_cost_history"),
        ("gold", "weekly_margin"),
        ("quality", "price_quarantine"),
    )

    with duckdb.connect(str(db), read_only=True) as con:
        built_tables = {
            (namespace, name): con.execute(
                f'SELECT * FROM "{namespace}"."{name}"'
            ).fetch_arrow_table()
            for namespace, name in outputs
        }

    namespaces = {item[0] for item in catalog.list_namespaces()}

    for identifier, new_data in built_tables.items():
        namespace, name = identifier
        if namespace not in namespaces:
            catalog.create_namespace(namespace)
            namespaces.add(namespace)

        if not catalog.table_exists(identifier):
            table = catalog.create_table(identifier=identifier, schema=new_data.schema)
            table.append(new_data)
            print(f"Created {namespace}.{name}")
            continue

        table = catalog.load_table(identifier)
        current_data = table.scan().to_arrow()

        if _same_rows(current_data, new_data):
            print(f"Unchanged {namespace}.{name}")
            continue

        # This updates the existing Iceberg table, so its older snapshots stay available.
        table.overwrite(new_data)
        print(f"Updated {namespace}.{name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    args = parser.parse_args()

    args.warehouse.mkdir(parents=True, exist_ok=True)
    db = args.warehouse / "qafila.duckdb"

    ingest(args.data_dir, db)
    transform(db)
    publish(db, args.warehouse)
    print(f"done: {args.warehouse.resolve()}")


if __name__ == "__main__":
    main()
