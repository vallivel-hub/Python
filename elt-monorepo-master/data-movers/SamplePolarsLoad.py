#
# Simple example program of loading tables from oracle to SQL server.
#
# ==========================================================
# Single-table-per-task ETL loader from Oracle → SQL Server
# ==========================================================
#
# Key features:
#   • Loads Oracle tables into SQL Server (one task per table).
#   • Uses Polars for fast column handling & type casting.
#   • Scheduler can run in either ThreadPoolExecutor (default)
#     or ProcessPoolExecutor mode (toggle via USE_PROCESSES=1).
#   • Handles Oracle CLOB/LONG/TIMESTAMP columns by converting
#     them to safe string representations.
#   • Adds an ingestion timestamp column to every target table.
#   • Cleans up memory aggressively (important with Polars).
#
# History / Gotchas (documenting issues we solved):
#   • USE_PROCESSES must be read *after* .env is loaded
#     (otherwise it always defaults to False).
#   • Multiprocessing requires all scheduled functions
#     to be *picklable*. This is why we introduced a thin
#     wrapper `process_table_task()` that lives at module
#     top level.
#   • Oracle CLOB/LONG fields are not directly usable;
#     we convert them via `TO_CHAR(SUBSTR(...))` in queries.
#   • Oracle TIMESTAMP/DATE fields are also converted to
#     strings before loading into Polars, then re-cast.
#   • Without memory cleanup (gc.collect()), long runs will
#     bloat memory usage significantly.
#   • Staging tables are dropped/recreated on each run to
#     guarantee schema consistency.
#

import sys
import gc
import polars as pl
import datetime

from db_utils.scheduler import AdaptiveScheduler
from db_utils.db_connector import get_db_connection
from db_utils.env_config import (
    parse_run_environment_from_args,
    load_environment_dotenv,
    load_config,
)
from db_utils.logger import get_logger, alert_on_crash

# -------- Target/source databases (globals) --------
targetdb = 'UMS_DBT'
sourcedb = 'CSRPT'
targetschema = 'peoplesoft-cs'
targetdblist = 'ELT_MetaData'
targetdbsql = 'ELT_MetaData'  # kept for completeness


# -------- Type mapping helpers --------
def map_oracle_to_sqlserver(col_name, data_type, data_length, data_precision, data_scale):
    """Maps Oracle column definitions → SQL Server types."""
    dt = (data_type or "").upper()
    if dt in ('CHAR', 'NCHAR'):
        return f"[{col_name}] NCHAR({data_length})"
    elif dt == 'VARCHAR2':
        return f"[{col_name}] VARCHAR({data_length})"
    elif dt == 'NVARCHAR2':
        return f"[{col_name}] NVARCHAR({data_length})"
    elif dt == 'DATE':
        return f"[{col_name}] DATETIME2(3)"
    elif dt in ('TIMESTAMP(6)', 'TIMESTAMP(9)', 'TIMESTAMP'):
        return f"[{col_name}] DATETIME2(3)"
    elif dt in ('CLOB', 'LONG'):
        return f"[{col_name}] VARCHAR(4000)"   # safe truncation
    elif dt == 'BLOB':
        return f"[{col_name}] VARBINARY(MAX)"  # THIS will probably be an issue?
    elif dt == 'NUMBER' and data_precision is None:
        return f"[{col_name}] INT"
    elif dt == 'NUMBER' and data_precision is not None:
        return f"[{col_name}] DECIMAL({data_precision},{data_scale or 0})"
    else:
        return f"[{col_name}] NVARCHAR(255)"


def map_oracle_to_polars_dtype(data_type, data_precision=None, data_scale=None):
    """Maps Oracle types → Polars dtypes (safe load & cast)."""
    dt = (data_type or "").upper()
    if dt in ("CHAR", "NCHAR", "VARCHAR2", "NVARCHAR2", "CLOB", "LONG"):
        return pl.Utf8
    elif dt == "DATE":
        return pl.Datetime("us")
    elif dt.startswith("TIMESTAMP"):
        return pl.Datetime("us")
    elif dt == "BLOB":
        return pl.Binary
    elif dt == "NUMBER":
        if data_scale and data_scale > 0:
            return pl.Float64
        elif data_precision and data_precision < 10:
            return pl.Int32
        elif data_precision and data_precision < 19:
            return pl.Int64
        else:
            return pl.Float64
    else:
        return pl.Utf8


# -------- Top-level worker function --------
def process_table(run_environment, configDB, tbl, where_clause=None):
    """
    Process a single table:
      1. Reads Oracle rows (with safe string conversions).
      2. Creates/replaces staging table in SQL Server.
      3. Streams Oracle rows in chunks, normalizes with Polars.
      4. Casts columns safely to target dtypes.
      5. Bulk inserts with pyodbc fast_executemany.
    Returns: (table name, elapsed time, total rows).
    """
    import time
    logger = get_logger("OracleToSQLServer", run_environment)

    tbl_start = time.time()
    total_rows = 0
    cast_failures = 0

    target_table = tbl  # staging name in SQL Server
    logger.info(f"Processing base table: {tbl}, staging target: {target_table}")

    # Oracle + SQL Server connections
    conn_oracle = get_db_connection(run_environment, sourcedb, logger)
    cur_oracle = conn_oracle.cursor()
    cur_oracle.arraysize = 10000

    conn_sql = get_db_connection(run_environment, targetdb, logger)
    cur_sql = conn_sql.cursor()

    try:
        logger.info(f"=== Processing table {target_table} ===")

        # Get Oracle metadata (columns, types, precision, etc.)
        cur_oracle.execute(configDB[sourcedb]["sql"], (tbl, tbl))
        metadata_rows = cur_oracle.fetchall()

        # Build schema maps
        schema_map = {row[1]: map_oracle_to_polars_dtype(row[2], row[4], row[5])
                      for row in metadata_rows}
        date_cols = [row[1] for row in metadata_rows if (row[2] or "").upper() == "DATE"]
        timestamp_cols = [row[1] for row in metadata_rows if (row[2] or "").upper().startswith("TIMESTAMP")]

        # Drop + recreate staging table in SQL Server
        logger.info(f"Ensuring SQL Server table {target_table} exists fresh...")
        cur_sql.execute(f"""
            IF OBJECT_ID('[{targetschema}].[{target_table}]', 'U') IS NOT NULL
                DROP TABLE [{targetschema}].[{target_table}];
        """)
        conn_sql.commit()
        create_table_in_sqlserver(cur_sql, target_table, metadata_rows, logger)
        conn_sql.commit()

        # Build Oracle query with safe conversions
        select_cols = []
        for row in metadata_rows:
            col_name = row[1]
            data_type = (row[2] or "").upper()
            if data_type in ("CLOB", "LONG"):
                # Oracle CLOB/LONG → VARCHAR
                select_cols.append(f"TO_CHAR(SUBSTR({col_name}, 1, 3999)) AS {col_name}")
            elif data_type == "DATE":
                # Oracle DATE → VARCHAR
                select_cols.append(f"TO_CHAR({col_name}, 'YYYY-MM-DD HH24:MI:SS') AS {col_name}")
            elif data_type.startswith("TIMESTAMP"):
                # Oracle TIMESTAMP → VARCHAR
                select_cols.append(f"TO_CHAR({col_name}, 'YYYY-MM-DD HH24:MI:SS.FF3') AS {col_name}")
            else:
                select_cols.append(col_name)

        oracle_query = f"SELECT {', '.join(select_cols)} FROM sysadm.{tbl}"
        if where_clause:
            oracle_query += f" WHERE {where_clause}"
        logger.info(f"Oracle query for {target_table}: {oracle_query}")

        # Stream Oracle rows in chunks, 50000 seems to be a sweet spot
        cur_oracle.execute(oracle_query)
        batch_size = 50000
        cols = [desc[0] for desc in cur_oracle.description]
        chunk_idx = 0
        ingestion_timestamp = datetime.datetime.now()

        while True:
            rows = cur_oracle.fetchmany(batch_size)
            if not rows:
                break
            chunk_idx += 1
            total_rows += len(rows)
            logger.info(f"[{target_table}] Processing chunk {chunk_idx} with {len(rows)} rows")

            # Normalize → Polars DataFrame
            norm_rows = [[str(val) if val is not None else None for val in row] for row in rows]
            df_chunk = pl.DataFrame(norm_rows, schema=[(c, pl.Utf8) for c in cols], orient="row")

            # Cast columns back to correct Polars dtypes
            casts = []
            for col in df_chunk.columns:
                if col in date_cols:
                    try:
                        casts.append(pl.col(col).str.strptime(pl.Datetime("us"),
                                                              "%Y-%m-%d %H:%M:%S", strict=False))
                    except Exception as e:
                        cast_failures += 1
                        logger.warning(f"[{target_table}] Could not cast DATE {col}: {e}")
                elif col in timestamp_cols:
                    try:
                        casts.append(pl.col(col).str.strptime(pl.Datetime("us"),
                                                              "%Y-%m-%d %H:%M:%S.%3f", strict=False))
                    except Exception as e:
                        cast_failures += 1
                        logger.warning(f"[{target_table}] Could not cast TIMESTAMP {col}: {e}")
                elif col in schema_map:
                    try:
                        casts.append(pl.col(col).cast(schema_map[col], strict=False))
                    except Exception as e:
                        logger.warning(f"[{target_table}] Could not cast {col} to {schema_map[col]}: {e}")

            if casts:
                df_chunk = df_chunk.with_columns(casts)

            # Prepend ingestion timestamp
            ingestion_col = pl.Series("IngestionDateTime", [ingestion_timestamp] * df_chunk.height)
            df_chunk = df_chunk.insert_column(0, ingestion_col)

            # Bulk insert into SQL Server
            bulk_insert_with_pyodbc(conn_sql, target_table, df_chunk, logger, schema_name=targetschema)
            logger.info(f"[{target_table}] Finished chunk {chunk_idx} ({len(rows)} rows)")

            # Cleanup memory (important for long runs)
            del df_chunk, rows, norm_rows
            gc.collect()

        # Summary
        elapsed = time.time() - tbl_start
        logger.info(
            f"=== Finished {target_table} in {elapsed:.2f}s, {total_rows} rows, "
            f"{len(date_cols) + len(timestamp_cols)} datetime cols, {cast_failures} cast failures ==="
        )
        return target_table, elapsed, total_rows

    finally:
        # Defensive cleanup
        try: cur_oracle.close(); conn_oracle.close()
        except Exception: pass
        try: cur_sql.close(); conn_sql.close()
        except Exception: pass


# -------- Helpers --------
def create_table_in_sqlserver(ss_cursor, table_name, metadata_rows, logger):
    """Drops and recreates the SQL Server table based on Oracle metadata."""
    col_defs = [
        map_oracle_to_sqlserver(
            col_name=row[1],
            data_type=row[2],
            data_length=row[3],
            data_precision=row[4],
            data_scale=row[5],
        )
        for row in metadata_rows
    ]
    ddl = (
            f"IF OBJECT_ID('[{targetschema}].[{table_name}]', 'U') IS NOT NULL "
            f"DROP TABLE [{targetschema}].[{table_name}];\n"
            f"CREATE TABLE [{targetschema}].[{table_name}] (\n"
            f"    [IngestionDateTime] DATETIME2(3) NOT NULL,\n    " +
            ",\n    ".join(col_defs) +
            "\n);"
    )
    logger.info(f"Creating table [{targetschema}].[{table_name}] in SQL Server...")
    ss_cursor.execute(ddl)


def bulk_insert_with_pyodbc(conn_sql, table_name, df_chunk: pl.DataFrame, logger, schema_name=targetschema):
    """Insert a Polars DataFrame chunk into SQL Server using pyodbc fast_executemany."""
    if df_chunk.height == 0:
        return
    placeholders = ", ".join(["?"] * len(df_chunk.columns))
    col_names = ", ".join(f"[{col}]" for col in df_chunk.columns)
    insert_stmt = f"INSERT INTO [{schema_name}].[{table_name}] ({col_names}) VALUES ({placeholders})"
    cursor = conn_sql.cursor()
    cursor.fast_executemany = True
    logger.info(f"Inserting {df_chunk.height} rows into {schema_name}.{table_name}...")
    rows_as_tuples = df_chunk.rows(named=False)
    cursor.executemany(insert_stmt, rows_as_tuples)
    conn_sql.commit()
    cursor.close()


# ====== Utility thin wrapper for scheduler compatibility ======
# Why? Multiprocessing requires all callables to be picklable.
# Having a top-level thin wrapper ensures compatibility.
def process_table_task(run_environment, configDB, table_name, where_clause):
    """Thin picklable wrapper used by scheduler (keeps same call signature)."""
    return process_table(run_environment, configDB, table_name, where_clause)


# -------- Main entry point --------
def main(run_environment=None, use_processes=None):
    """
    Main orchestrator: submits tables to AdaptiveScheduler.
      • use_processes=True → ProcessPoolExecutor
      • use_processes=False → ThreadPoolExecutor
    """
    import os
    import time

    run_environment = run_environment or os.getenv("RUN_ENV") or parse_run_environment_from_args()
    load_environment_dotenv(run_environment, logger=None)
    logger = get_logger("OracleToSQLServer", run_environment)
    print("USE_PROCESSES:", os.getenv("USE_PROCESSES"))

    # If caller didn’t pass explicitly, check .env
    if use_processes is None:
        use_processes = os.getenv("USE_PROCESSES", "0") == "1"

    # --- Startup log (confirms mode) ---
    mode = "ProcessPoolExecutor" if use_processes else "ThreadPoolExecutor"
    logger.info(f"[Startup] USE_PROCESSES={use_processes} → {mode}")

    @alert_on_crash(logger)
    def _main(run_environment, use_processes):
        start_time = time.time()
        _, configDB, _ = load_config(logger, run_environment)

        # Step 1: Table list from metadata
        conn_sql = get_db_connection(run_environment, targetdb, logger)
        cur_sql = conn_sql.cursor()
        cur_sql.execute(configDB[targetdblist]["sqltablelist"])
        tables = [row.TABLE_NAME for row in cur_sql.fetchall()]
        cur_sql.close(); conn_sql.close()
        logger.info(f"Found {len(tables)} tables to process: {tables}")

        # Step 2: Oracle row counts (used for sorting large→small)
        conn_oracle = get_db_connection(run_environment, sourcedb, logger)
        cur_oracle = conn_oracle.cursor()
        size_query = f"""
            SELECT table_name, num_rows
            FROM all_tables
            WHERE owner = 'SYSADM'
              AND table_name IN ({",".join(["'" + t + "'" for t in tables])})
        """
        cur_oracle.execute(size_query)
        size_map = {row[0]: row[1] for row in cur_oracle.fetchall()}
        cur_oracle.close(); conn_oracle.close()

        # Step 3: Prepare tasks
        tables_with_size = [(tbl, size_map.get(tbl.upper(), 0)) for tbl in tables]
        tables_sorted = sorted(tables_with_size, key=lambda x: x[1] or 0, reverse=True)
        logger.info(f"Tables sorted by estimated row count: {tables_sorted}")
        tasks = [(tbl, process_table, (run_environment, configDB, tbl)) for tbl, _ in tables_sorted]

        # Step 4: Run with AdaptiveScheduler
        scheduler = AdaptiveScheduler(
            max_workers=8,
            min_workers=2,
            adjust_interval=60,
            logger=logger,
            use_processes=use_processes,
        )
        results = scheduler.submit_tasks(tasks)

        # Step 5: Log summary
        total_time = time.time() - start_time
        logger.info(f"All tables processed in {total_time:.2f}s")
        for tbl, elapsed, total_rows in results:
            logger.info(f"Summary: {tbl} → {total_rows} rows in {elapsed:.2f}s")

    # Call the inner runner
    _main(run_environment, use_processes)


if __name__ == "__main__":
    import os
    # Load .env early so USE_PROCESSES is available
    run_environment = os.getenv("RUN_ENV") or parse_run_environment_from_args()
    load_environment_dotenv(run_environment, logger=None)

    # Check toggle
    use_processes = os.getenv("USE_PROCESSES", "0") == "1"

    # Kick off main
    if len(sys.argv) > 1:
        main(sys.argv[1], use_processes=use_processes)
    else:
        main(use_processes=use_processes)