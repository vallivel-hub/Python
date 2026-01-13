#
# Base processing program of loading tables from oracle to SQL server
# to be used by dbt, dimensional models, and other downstream processes.
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
from zoneinfo import ZoneInfo  # available in Python 3.9+

from db_utils.scheduler import AdaptiveScheduler
from db_utils.db_connector import get_db_connection
from db_utils.env_config import (
    parse_run_environment_from_args,
    load_environment_dotenv,
    load_config,
)
from db_utils.logger import get_logger, alert_on_crash
from scd_schema_sync import run_scd2_sync

# -------- Target/source databases (globals) --------

sourcedb = 'CS' # The tag for the database connection in dataBaseConnections.yaml AND used to get oracle sql from yaml file.
reportingschema = 'reporting' # schema name used to create sql to make reporting objects
martschema = 'mart' # schema name used to create sql to make mart objects
targetdb = 'UMS_DBT' # DB and yaml tag to load the data into
targetschema = 'pscs ' # The schema we will load the tables into pscs is the production/dev standard.
metadatadb = 'ELT_MetaData' # Always used to connedt to the Metadata database and yaml entries.

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
# ----------------------------
# process_table and related helpers
# ----------------------------

def process_table(run_environment, configDB, tbl, where_clause=None):
    """
    Top-level worker for one Oracle -> SQL Server table.
    Keeps your original signature. Key flow:
      - load metadata from Oracle
      - optionally fetch oracle_hashkey expression from metadata repo
      - create staging table in SQL Server (drop/create)
      - stream Oracle rows (safe string conversions), convert to Polars DataFrame
      - cast back to strong dtypes, prepend ingestion timestamp, ensure hashkey column exists
      - normalize duplicate column names (case-insensitive)
      - bulk insert using pyodbc fast_executemany
      - create clustered index & hash index
      - create reporting view
      - optionally create dim view / SCD2 objects (when DIRECT_DIM_TABLE = 'Y' in metadata)
    Returns (table_name, elapsed_seconds, rows_inserted)
    """
    import time
    logger = get_logger("process_table", run_environment)

    tbl_start = time.time()
    total_rows = 0

    target_table = tbl
    logger.info(f"Processing base table: {tbl} → staging {target_table}")

    # Connections: oracle for source, SQL Server for target & metadata
    conn_oracle = get_db_connection(run_environment, sourcedb, logger)
    cur_oracle = conn_oracle.cursor()
    cur_oracle.arraysize = 10000

    conn_sql = get_db_connection(run_environment, targetdb, logger)
    cur_sql = conn_sql.cursor()

    try:
        logger.info(f"=== Processing table {target_table} ===")

        # --- Oracle metadata (columns, types, precision...) ---
        cur_oracle.execute(configDB[sourcedb]["sql"], (tbl, tbl))
        metadata_rows = cur_oracle.fetchall()
        if not metadata_rows:
            logger.warning(f"No metadata returned for table {tbl}; skipping.")
            return tbl, 0.0, 0

        # --- Fetch oracle hash expression from metadata table (if any) ---
        oracle_hashkey_column = get_oracle_hashkey_for_table(run_environment, configDB, tbl, logger)
        if oracle_hashkey_column:
            logger.info(f"Found oracle hashkey expression for {tbl}: {oracle_hashkey_column}")

        # --- Build mappings for casting later in Polars ---
        schema_map = {row[1]: map_oracle_to_polars_dtype(row[2], row[4], row[5])
                      for row in metadata_rows}
        date_cols = [row[1] for row in metadata_rows if (row[2] or "").upper() == "DATE"]
        timestamp_cols = [row[1] for row in metadata_rows if (row[2] or "").upper().startswith("TIMESTAMP")]

        # --- Create/replace the target staging table in SQL Server ---
        logger.info(f"Ensuring SQL Server table {target_table} exists fresh...")
        cur_sql.execute(f"""
            IF OBJECT_ID('[{targetschema}].[{target_table}]', 'U') IS NOT NULL
                DROP TABLE [{targetschema}].[{target_table}];
        """)
        conn_sql.commit()
        create_table_in_sqlserver(cur_sql, target_table, metadata_rows, logger, oracle_hashkey_column)
        conn_sql.commit()

        # --- Build Oracle SELECT with safe conversions (CLOB, DATE, TIMESTAMP → VARCHAR) ---
        select_cols = []
        for row in metadata_rows:
            col_name = row[1]
            data_type = (row[2] or "").upper()
            if data_type in ("CLOB", "LONG"):
                select_cols.append(f"TO_CHAR(SUBSTR({col_name}, 1, 3999)) AS {col_name}")
            elif data_type == "DATE":
                select_cols.append(f"TO_CHAR({col_name}, 'YYYY-MM-DD HH24:MI:SS') AS {col_name}")
            elif data_type.startswith("TIMESTAMP"):
                select_cols.append(f"TO_CHAR({col_name}, 'YYYY-MM-DD HH24:MI:SS.FF3') AS {col_name}")
            else:
                select_cols.append(col_name)

        # If oracle_hashkey_column exists, ensure we insert the proper expression.
        # The metadata may contain either an expression already with "AS <alias>" or just the expression.
        if oracle_hashkey_column:
            # If the expression already has an alias (AS ...), use as-is.
            # Otherwise create an alias "Hash_<TABLE>_Key" to match SQL Server naming convention.
            if " AS " in oracle_hashkey_column.upper():
                select_cols.insert(0, oracle_hashkey_column)
            else:
                select_cols.insert(0, f"{oracle_hashkey_column} AS Hash_{tbl}_Key")
        else:
            logger.debug(f"No hashkey column for {tbl}")

        oracle_query = f"SELECT {', '.join(select_cols)} FROM sysadm.{tbl}"
        if where_clause:
            oracle_query += f" WHERE {where_clause}"
        logger.info(f"Oracle query for {target_table}: {oracle_query}")
        cur_oracle.execute(oracle_query)

        # --- Streaming loop ---
        batch_size = 50000
        cols = [desc[0] for desc in cur_oracle.description]
        chunk_idx = 0
        ingestion_timestamp = datetime.datetime.now(ZoneInfo("America/New_York"))

        while True:
            rows = cur_oracle.fetchmany(batch_size)
            if not rows:
                break
            chunk_idx += 1
            total_rows += len(rows)
            logger.info(f"[{target_table}] Processing chunk {chunk_idx} with {len(rows)} rows")

            # Normalize row values to strings/None to help Polars construct DF reliably
            norm_rows = [[str(val) if val is not None else None for val in row] for row in rows]

            # Build Polars DataFrame, specifying column names (as strings). If Polars raises ShapeError,
            # it means some rows had different width — we handle that earlier via normalization or dropping.
            df_chunk = pl.DataFrame(norm_rows, schema=[(c, pl.Utf8) for c in cols], orient="row")

            # Cast columns back to their desired Polars dtypes (dates/timestamps/numerics)
            casts = []
            for col in df_chunk.columns:
                if col in date_cols:
                    casts.append(pl.col(col).str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S", strict=False))
                elif col in timestamp_cols:
                    casts.append(pl.col(col).str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S.%3f", strict=False))
                elif col in schema_map:
                    casts.append(pl.col(col).cast(schema_map[col], strict=False))
            if casts:
                df_chunk = df_chunk.with_columns(casts)

            # Prepend IngestionDateTime column (at position 0)
            ingestion_col = pl.Series("IngestionDateTime", [ingestion_timestamp] * df_chunk.height)
            df_chunk = df_chunk.insert_column(0, ingestion_col)

            # If an oracle hashkey expression existed but Oracle failed to compute it (no column returned),
            # ensure the Hash_<table>_Key column exists (filled with None) so SQL Server table layout matches insert.
            expected_hash_col = f"Hash_{tbl}_Key"
            if oracle_hashkey_column and expected_hash_col not in df_chunk.columns:
                df_chunk = df_chunk.insert_column(1, pl.Series(expected_hash_col, [None] * df_chunk.height))

            # Remove duplicate column names (case-insensitive) - keeps the first occurrence
            df_chunk = normalize_df_columns(df_chunk, logger)

            # Insert into SQL Server
            bulk_insert_with_pyodbc(conn_sql, target_table, df_chunk, logger, schema_name=targetschema.strip())
            logger.info(f"[{target_table}] Finished chunk {chunk_idx}")

            # free memory
            del df_chunk, rows, norm_rows
            gc.collect()

        # --- After all chunks: create indexes, views, SCD objects ---
        # Rebuild clustered index after full table load
        create_clustered_index_for_loaded_table(conn_sql, cur_sql, configDB, logger, metadatadb, targetschema.strip(), target_table)

        # Get actual column list from SQL Server (for reporting view creation)
        columns = get_table_columns(conn_sql, configDB, logger, targetdb, targetschema.strip(), target_table)

        # Get columns used for DIM (from metadata)
        dim_columns = get_DIM_columns_for_table(cur_sql, configDB, metadatadb, target_table)

        # DIRECT_DIM_TABLE flag from oracle_metadata
        cur_sql.execute(configDB[metadatadb]['sqldimensiontable'], target_table)
        dim_flag_row = cur_sql.fetchone()
        direct_dim_flag = dim_flag_row[0] == 'Y' if dim_flag_row else False
        logger.debug(f"[{target_table}] DIRECT_DIM_TABLE flag: {direct_dim_flag}")


        columns_types = get_columns_and_types_for_table(cur_sql, configDB, metadatadb, target_table)

        # Recreate reporting view
        truncate_and_create_reporting_view(conn_sql, cur_sql, reportingschema.strip(), targetschema.strip(), target_table, columns,  logger)

        # Get hashkey (SQL Server stored version) using cur_sql
        hashkey_column = get_hashkey_for_table(cur_sql, configDB, metadatadb, target_table)

        # Dimension / SCD2 logic (if flagged)
        try:
            if direct_dim_flag:
                logger.info(f"[{target_table}] DIRECT_DIM_TABLE → generating DIM + SCD2 objects")

                truncate_and_create_dim_view(
                    conn_sql=conn_sql,
                    cur_sql=cur_sql,
                    martschema=martschema.strip(),
                    table_name=target_table,
                    run_environment=run_environment,
                    target_db=targetdb,       # ✅ Your intended DB
                    target_schema=targetschema.strip(),
                    columns=dim_columns,
                    columns_types=columns_types,
                    dim_view=True,
                    hashkey_column=hashkey_column,
                    logger=logger
                )

                logger.info(f"[{target_table}] ✅ Dimension & SCD2 processing complete.")
            else:
                logger.debug(f"[{target_table}] Skipping DIM/SCD - DIRECT_DIM_TABLE flag not enabled.")
        except Exception as e:
            logger.error(f"[{target_table}] ❌ Error in DIM/SCD logic: {e}")

        elapsed = time.time() - tbl_start
        logger.info(f"=== Finished {target_table} in {elapsed:.2f}s, {total_rows} rows ===")
        return target_table, elapsed, total_rows

    finally:
        # defensive cleanup of cursors/conns
        try:
            cur_oracle.close()
            conn_oracle.close()
        except Exception:
            pass
        try:
            cur_sql.close()
            conn_sql.close()
        except Exception:
            pass

# --------------------
# Helper: create target table in SQL Server (drop & create)
# --------------------
def create_table_in_sqlserver(ss_cursor, table_name, metadata_rows, logger, hashkey_column=None):
    """
    Create (drop if exists) a SQL Server staging table based on Oracle metadata.
    If hashkey_column is passed we add [Hash_<table>_Key] CHAR(32) column after ingestion datetime.
    """
    # Build column DDLs from oracle metadata
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

    # Add prefix columns
    if hashkey_column:
        prefix = f"[IngestionDateTime] DATETIME2(7) NOT NULL, [Hash_{table_name}_Key] CHAR(32), "
    else:
        prefix = f"[IngestionDateTime] DATETIME2(7) NOT NULL, "

    ddl_statements = []

    # 1. Conditionally TRUNCATE TABLE if it exists
    # This is safe because IF OBJECT_ID checks existence
    truncate_sql = (
        f"IF OBJECT_ID('[{targetschema.strip()}].[{table_name}]', 'U') IS NOT NULL "
        f"TRUNCATE TABLE [{targetschema.strip()}].[{table_name}];"
    )
    ddl_statements.append(truncate_sql)

    # 2. Conditionally DROP TABLE if it exists
    # The table will be dropped immediately after potential truncation
    drop_sql = (
        f"IF OBJECT_ID('[{targetschema.strip()}].[{table_name}]', 'U') IS NOT NULL "
        f"DROP TABLE [{targetschema.strip()}].[{table_name}];"
    )
    ddl_statements.append(drop_sql)

    # 3. CREATE TABLE
    create_sql = (
        f"CREATE TABLE [{targetschema.strip()}].[{table_name}] (\n"
        f"    {prefix}" + ",\n    ".join(col_defs) + "\n);"
    )
    ddl_statements.append(create_sql)

    # Join the statements with a semicolon and a newline to ensure separation
    # Some drivers/environments might require a dedicated call for each, but this is a common approach.
    ddl = "\n".join(ddl_statements)

    logger.info(f"Preparing to TRUNCATE, DROP, and CREATE table [{targetschema.strip()}].[{table_name}] in SQL Server...")
    ss_cursor.execute(ddl)


# --------------------
# Helper: bulk insert Polars DataFrame into SQL Server using pyodbc fast_executemany
# --------------------
def bulk_insert_with_pyodbc(conn_sql, table_name, df_chunk: pl.DataFrame, logger, schema_name=targetschema.strip()):
    """
    Insert a Polars DataFrame chunk into SQL Server using pyodbc fast_executemany.
    Assumes df_chunk columns are already normalized (unique) and in the correct order.
    """
    if df_chunk.height == 0:
        logger.debug("No rows in df_chunk, skipping insert.")
        return

    placeholders = ", ".join(["?"] * len(df_chunk.columns))
    col_names = ", ".join(f"[{col}]" for col in df_chunk.columns)
    insert_stmt = f"INSERT INTO [{schema_name}].[{table_name}] ({col_names}) VALUES ({placeholders})"

    cursor = conn_sql.cursor()
    cursor.fast_executemany = True
    logger.info(f"Preparing to insert {df_chunk.height} rows into {schema_name}.{table_name}...")
    rows_as_tuples = df_chunk.rows(named=False)

    try:
        cursor.executemany(insert_stmt, rows_as_tuples)
        conn_sql.commit()
        logger.info(f"Inserted {len(rows_as_tuples)} rows into {schema_name}.{table_name}")
    except Exception as e:
        # Log the failing SQL and a few sample rows to help debugging
        logger.error(f"Bulk insert failed for {schema_name}.{table_name}: {e}")
        logger.error(f"Insert statement: {insert_stmt}")
        for i, sample in enumerate(rows_as_tuples[:5]):
            logger.error(f"Sample row {i}: {sample}")
        raise
    finally:
        try:
            cursor.close()
        except Exception:
            pass


# --------------------
# Helper: fetch ORACLE_HASHKEY_CODE from metadata (opens/closes its own connection)
# --------------------
def get_oracle_hashkey_for_table(run_environment, configDB, table_name, logger):
    """
    Connect to metadata DB and return the ORACLE_HASHKEY_CODE expression for the table (if present).
    Returns the text exactly as stored (should already contain 'AS alias').
    """
    conn_sql = None
    cur_sql = None

    try:
        # Connect to SQL Server metadata repository
        conn_sql = get_db_connection(run_environment, metadatadb, logger)
        cur_sql = conn_sql.cursor()

        # Run the query to get oracle_hashkey_code
        cur_sql.execute(configDB[metadatadb]['sqloraclehashcode'], (table_name,))
        row = cur_sql.fetchone()

        if not row:
            logger.warning(f"No ORACLE_HASHKEY_CODE found for table [{table_name}].")
            return None

        # support named attribute or positional access
        if hasattr(row, "ORACLE_HASHKEY_CODE"):
            return row.ORACLE_HASHKEY_CODE
        # fallback: treat first column as the expression
        return row[0] if len(row) > 0 else None

    except Exception as e:
        logger.warning(f"Failed to fetch ORACLE_HASHKEY_CODE for {table_name}: {e}")
        return None
    finally:
        try:
            if cur_sql:
                cur_sql.close()
            if conn_sql:
                conn_sql.close()
        except Exception:
            pass


# --------------------
# Helper: get SQL Server-stored HASHKEY_CODE using existing cursor
# --------------------
def get_hashkey_for_table(cur_sql, configDB, metadatadb, table_name):
    """
    Retrieve the HASHKEY_CODE (SQL Server version) where COLUMN_ID is '1' from oracle_metadata table.
    This function expects cur_sql (open cursor) and will NOT open/close a connection.
    """
    # Ensure parameter is a single-element tuple for pyodbc DB-API
    cur_sql.execute(configDB[metadatadb]['sqlhashcode'], (table_name))

    row = cur_sql.fetchone()  # Fetch only one row

    # prefer attribute access if available
    if hasattr(row, "HASHKEY_CODE"):
        return row.HASHKEY_CODE
    return row[0] if len(row) > 0 else None


# --------------------
# Helper: normalize Polars DataFrame column names (case-insensitive)
# --------------------
def normalize_df_columns(df: pl.DataFrame, logger):
    """
    Remove duplicate columns by case-insensitive name, keep first occurrence.
    Return a new DataFrame with the de-duplicated column set in their original order.
    This avoids SQL Server errors where the same column name (different case) appears twice.
    """
    seen = set()
    keep = []
    for col in df.columns:
        cu = col.upper()
        if cu in seen:
            logger.debug(f"Dropping duplicate column (case-insensitive): {col}")
            continue
        seen.add(cu)
        keep.append(col)
    # select preserves order of keep[]
    return df.select(keep)


# --------------------
# Indexing helpers (clustered / nonclustered hash index)
# --------------------
def get_unique_columns_for_table(cur_sql, configDB, metadatadb, table_name):
    """Return list of unique-key columns (COLUMN_NAME) where unique_key_column = 'Y' from metadata."""
    cur_sql.execute(configDB[metadatadb]['sqluniquecolumns'], table_name)
    return [row.COLUMN_NAME for row in cur_sql.fetchall()]


def create_table_clustered_index(cur_sql, conn_sql, logger, target_schema, table_name, unique_columns):
    """Create a clustered index using the given unique columns (if present)."""
    if not unique_columns:
        logger.warning(f"[{table_name}] Skipping clustered index creation — no unique columns marked 'Y'.")
        return

    columns_str = ", ".join([f"[{col}]" for col in unique_columns])
    index_name = f"CLIX__{target_schema}__{table_name}"
    logger.info(f"[{table_name}] Creating clustered index {index_name} on ({columns_str})...")

    cur_sql.execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes WHERE name = '{index_name}' AND object_id = OBJECT_ID('{target_schema}.{table_name}')
        )
        BEGIN
            CREATE CLUSTERED INDEX [{index_name}] 
            ON [{target_schema}].[{table_name}] ({columns_str})
        END
    """)

    conn_sql.commit()
    logger.info(f"[{table_name}] Clustered index creation complete.")


def create_clustered_index_for_loaded_table(conn_sql, cur_sql, configDB, logger, target_db, target_schema, table_name):
    """
    Wrapper that:
      - fetches unique columns from metadata (target_db)
      - creates clustered index if unique columns exist
      - creates a nonclustered index on the Hash_<table>_Key column (if present)
    """
    unique_columns = get_unique_columns_for_table(cur_sql, configDB, target_db, table_name)
    if not unique_columns:
        logger.warning(f"Skipping indexing for {table_name} as no columns are marked 'Y'.")
    else:
        try:
            create_table_clustered_index(cur_sql, conn_sql, logger, target_schema, table_name, unique_columns)
        except Exception as e:
            logger.error(f"[{table_name}] Error creating clustered index: {e}")

    # create nonclustered hashkey index (idempotent)
        hash_index_name = f"NCIX__{target_schema}__{table_name}__HashKey"
        try:
            cur_sql.execute(f"""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.indexes WHERE name = '{hash_index_name}' AND object_id = OBJECT_ID('{target_schema}.{table_name}')
                )
                BEGIN
                    CREATE NONCLUSTERED INDEX [{hash_index_name}]
                    ON [{target_schema}].[{table_name}] ([Hash_{table_name}_Key])
                END
            """)
            conn_sql.commit()
            logger.info(f"[{table_name}] Nonclustered hashkey index created (if missing).")
        except Exception as e:
            logger.error(f"[{table_name}] Error creating hashkey index: {e}")


# --------------------
# Get table columns from SQL Server INFORMATION_SCHEMA
# --------------------
def get_table_columns(conn_sql, configDB, logger, target_db, target_schema, table_name):
    """
    Return list of column names for the given table using metadata SQL in configDB.
    This uses the connection passed in (conn_sql).
    """
    try:
        cur = conn_sql.cursor()
        # some of your SQLs expect (table_name, schema) tuple
        cur.execute(configDB[target_db]['sqltablecolumns'], (table_name, target_schema))
        columns = [row.COLUMN_NAME for row in cur.fetchall()]
        cur.close()
        logger.debug(f"[{table_name}] Retrieved {columns} columns for reporting view.")
        return columns
    except Exception as e:
        logger.error(f"Failed to retrieve columns for {table_name}: {e}")
        return []


# --------------------
# Reporting view helper (drop & recreate)
# --------------------
def drop_view(cur_sql, view_name):
    """Drop view safely (if exists)."""
    cur_sql.execute(f"""
        IF OBJECT_ID('{view_name}', 'V') IS NOT NULL
            DROP VIEW {view_name};
    """)

def truncate_and_create_reporting_view(conn_sql, cur_sql, reportingschema, target_schema, table_name, columns, logger):
    """
    Create a reporting view for the supplied table. Exclude internal HASH_ columns to avoid clutter.
    Uses cur_sql for executing DDL and conn_sql for committing.
    """
    reporting_name = f"{reportingschema}.{table_name}_VW"
    filtered_columns = [col for col in columns if not col.upper().startswith("HASH_")]
    columns_str = ", ".join(f"[{col}]" for col in filtered_columns)

    drop_view(cur_sql, reporting_name)

    create_view_sql = f"""
        CREATE VIEW {reporting_name} AS 
        SELECT {columns_str} FROM [{target_schema}].[{table_name}]
    """
    cur_sql.execute(create_view_sql)
    conn_sql.commit()
    logger.info(f"[{table_name}] Reporting view created as {reporting_name}.")


# --------------------
# DIM + SCD helpers (focused)
#   These functions delegate to your existing run_scd2_sync implementation that you use
# --------------------
def truncate_and_create_dim_view(conn_sql, cur_sql, martschema, table_name, run_environment,
                                 target_db, target_schema, columns, columns_types,
                                 dim_view, hashkey_column, logger):
    """
    Create the dimension view and/or trigger creation of SCD2 table and merge logic.
    Expects cur_sql/conn_sql passed in (no globals).
    """
    if not dim_view:
        logger.warning(f"Skipping creating dimension view for {table_name} is not marked to be a dimension view.")
        return

    if hashkey_column is None:
        logger.warning(f"Skipping creating dimension view for {table_name} no unique key.")
        return

    # dimension view name (mart namespace) — remove PS_ prefix as you do
    dimension_name = f"{martschema}.DIM_{table_name}"
    dimension_name = dimension_name.replace("PS_", "")
    logger.debug(f"create dimension view - {dimension_name}")

    columns_str = ", ".join([f"[{col}]" for col in columns])
    stage_columns = columns_str.replace("[","S.[")
    logger.debug(f'insert columns SCD - {columns_str}')
    logger.debug(f'insert stage_columns SCD - {stage_columns}')

    hash_key = f"Hash_{table_name}_Key"
    hashdiff = f"HashDIFF_{table_name}"
    view_select_definition = f"[{hash_key}]," + columns_str
    scd_view_select_definition = f"[IngestionDateTime], [StartDate], [EndDate], [IsCurrent], [{hash_key}]," + columns_str
    # Build hashdiff expression(s)
    # Define the delimiter you want to use
    delimiter = "###"

    # 1. Create a list of the LTRIM(RTRIM(COALESCE(col, ''))) expressions
    #    without adding the delimiter to each individual element.
    column_expressions = [f"LTRIM(RTRIM(COALESCE({col}, '')))" for col in columns]
    column_sync_expressions = [f"LTRIM(RTRIM(COALESCE({col}, '''')))" for col in columns]

    # Handle the case of a single column
    if len(columns) == 1:
        hashdiff_columns_str = f"UPPER(CONVERT(CHAR(32), HASHBYTES('MD5', UPPER({column_expressions[0]})),2))"
        hashdiff_sync_columns_str = f"UPPER(CONVERT(CHAR(32), HASHBYTES(''MD5'', UPPER({column_sync_expressions[0]})),2))"
    else:
        # 2. Join these expressions using the delimiter string.
        #    The join string should include the SQL comma (for CONCAT arguments)
        #    and the SQL-escaped delimiter.
        concat_arguments_str  = f", '{delimiter}', ".join(column_expressions)
        concat_sync_arguments_str = f", ''{delimiter}'', ".join(column_sync_expressions)

        # 3. Construct the final HASHBYTES expression using the correctly joined arguments.
        #    The variable name `hashdiff_sync_columns_str` now correctly holds the full SQL expression.
        hashdiff_columns_str = f"UPPER(CONVERT(CHAR(32), HASHBYTES('MD5', UPPER(CONCAT( {concat_arguments_str}))),2))"
        hashdiff_sync_columns_str = f"UPPER(CONVERT(CHAR(32), HASHBYTES(''MD5'', UPPER(CONCAT( {concat_sync_arguments_str}))),2))"

    # If EFFDT present -> create a rolling view, else create SCD2 objects and view
    if "EFFDT" in columns:
        dimension_name = f"{dimension_name}_VW"
        drop_view(cur_sql, dimension_name)
        create_view_sql = f"""
                    CREATE VIEW {dimension_name} AS 
                    WITH OrderedData AS (
                        SELECT
                            [IngestionDateTime], {view_select_definition},
                            ROW_NUMBER() OVER(PARTITION BY {hash_key} ORDER BY [EFFDT]) AS rn,
                            LEAD([EFFDT], 1, NULL) OVER(PARTITION BY {hash_key} ORDER BY [EFFDT]) AS next_effectivedate
                        FROM [{target_schema}].{table_name}
                            where effdt <= GETDATE() 
                        )
                        SELECT
                            [IngestionDateTime],
                            [EFFDT] as StartDate,
                            CASE
                                WHEN od.next_effectivedate IS NULL THEN NULL -- Current record
                            ELSE DATEADD(day, -1, od.next_effectivedate) -- End date is next - 1 day
                            END AS EndDate,
                            CASE
                                WHEN od.next_effectivedate IS NULL THEN 1    -- Current record
                                ELSE 0                                       -- Historical record
                            END AS IsCurrent,
                            {view_select_definition}
                        FROM OrderedData od;
                """
        cur_sql.execute(create_view_sql)
        conn_sql.commit()
        logger.info(f"[{table_name}] EFFDT dimension view created: {dimension_name}")
    else:
        # Create or update SCD2 table and merge using existing stored procedure + scd sync routine
        create_fill_update_scd2_table(conn_sql, cur_sql, table_name, target_db, target_schema,
                                      run_environment, hash_key, hashdiff, columns_str,
                                      stage_columns, columns_types, hashkey_column,
                                      hashdiff_columns_str, hashdiff_sync_columns_str, logger)

        # Switch to the target_db database
        cur_sql.execute(f""" 
                        USE [{target_db}];    
                    """)

        # Drop & recreate scd view wrapper if necessary
        scd_dimension_name = f"{dimension_name}_VW"
        drop_view(cur_sql, scd_dimension_name)
        drop_view(cur_sql, dimension_name)
        create_view_sql = f"""
            IF OBJECT_ID('{scd_dimension_name}', 'V') IS NULL
            BEGIN
                DECLARE @SQL nvarchar(max);
                SET @SQL = N'
                    CREATE VIEW {scd_dimension_name} AS 
                    SELECT [IngestionDateTime], [StartDate], [EndDate], [IsCurrent], [{hash_key}], {columns_str}
                    FROM [{target_schema}].SCD_{table_name};
                ';
                EXEC sp_executesql @SQL;
            END;
        """
        cur_sql.execute(create_view_sql)
        conn_sql.commit()
        logger.info(f"[{table_name}] SCD2 dimension view created: {scd_dimension_name}")


def create_fill_update_scd2_table(conn_sql, cur_sql, table_name, target_db, target_schema, run_environment,
                                  hash_key, hashdiff, columns_str, stage_columns,
                                  columns_types, hashkey_column, hashdiff_columns_str, hashdiff_sync_columns_str, logger):
    """
    Create SCD2 table (if missing) and run your external scd sync routine
    (run_scd2_sync) to populate/merge data. This function expects connections/cursors passed in.
    """
    if not columns_types:
        logger.error(f"{table_name} – skipping SCD2 creation as no columns are marked 'Y'.")
        return

    scd_schema = f'[{target_schema}]'
    scd_stage_table = f"{table_name}"
    scd_target_table = f"SCD_{table_name}"

    columns_definition = ", ".join([f"{col}" for col in columns_types])
    if hashkey_column is not None:
        columns_definition = (f"[IngestionDateTime] DATETIME2(7), [StartDate] DATETIME2(7), [EndDate] DATETIME2(7), [IsCurrent] int not null, [{hash_key}] char(32), [{hashdiff}] char(32),"
                              + columns_definition )
    else:
        # fallback layout if no hashkey
        columns_definition = (f"[IngestionDateTime] DATETIME2(7) [{hashdiff}] char(32),"
                              + columns_definition )

    # Create SCD2 table if not created already.
    # Switch to the ELT_MetaData database
    cur_sql.execute(f"USE {target_db}")

    cur_sql.execute(f"""
                IF OBJECT_ID('[{target_schema}].{scd_target_table}', 'U') IS NULL
                    BEGIN
                        CREATE TABLE [{target_schema}].{scd_target_table} ({columns_definition});
                    END
                """)
    conn_sql.commit()

    # run external scd sync routine
    conn = get_db_connection(run_environment, target_db, logger)
    tables_to_process = [
        {'db': target_db,'schema': scd_schema, 'name': scd_stage_table, 'targetname': scd_target_table},
    ]
    run_scd2_sync(conn, tables_to_process, hashdiff, hashdiff_sync_columns_str,columns_definition, logger,dry_run=False)

    # Finally call merge stored procedure to produce final dimension merge
    try:
        logger.info(f"[{table_name}] Calling sp_merge_scd_dim stored proc...")

        # Switch to the ELT_MetaData database
        cur_sql.execute("USE ELT_MetaData")

        sql = """\
                    SET NOCOUNT ON
                    DECLARE @Out nvarchar(max);
                    EXEC [TSQL].[sp_merge_scd_dim] @InDB = ?, @InSchema = ?, @Intable_name = ?,@InHashColumnList = ?
                    ,@InColumnList = ?,@InStageColumnList = ?,@Inhashdiff = ?,@Inhashkey = ?, @OutResults = @Out OUTPUT;
                    SELECT @Out AS the_output;
                """

        cur_sql.execute(sql, (target_db,  # @InDB
                              target_schema,  # @InSchema
                              table_name,  # @Intable_name
                              hashdiff_columns_str,  # @InHashColumnList
                              columns_str,  # @InColumnList
                              stage_columns,  # @InStageColumnList
                              hashdiff,  # @Inhashdiff
                              hash_key))
        row = cur_sql.fetchone()
        # To retrieve the output parameter (@Inhashkey), you can access it after execution
        # OutResult = SQL_CURSOR.fetchone()[8]  # Assuming it's the 9th parameter
        if row and row[0] is not None:
            result_message = row[0]
            logger.debug(f"PROC DONE outresult = {result_message}")
        else:
            logger.warning(f"sp_merge_scd_dim returned no output for {table_name}")

        conn_sql.commit()

    except Exception as e:
        logger.error(f"❌ PROC FAILED for table {table_name}: {e}")
        conn_sql.rollback()

def get_columns_and_types_for_table(cur_sql, configDB, metadatadb, table_name):
    """Retrieve the column names where 'use_column' is 'Y' from ums_dv.business.oracle_metadata."""
    """Convert COLUMN_NAME to hold column_name and converted column_type.  Used for create tables."""
    # print(f"Getting columns and types for table {metadatadb} - {table_name}...")
    cur_sql.execute(configDB[metadatadb]['sqltypeconversioncolumns'], table_name)

    return [row.COLUMN_NAME for row in cur_sql.fetchall()]

def get_DIM_columns_for_table(cur_sql, configDB, metadatadb, table_name):
    """Retrieve the column names where 'use_column' is 'Y' from ums_dv.business.oracle_metadata."""
    # print(f"Getting DIM columns and types for table {metadatadb} - {table_name}...")
    cur_sql.execute(configDB[metadatadb]['sqlcolumns'], table_name)

    return [row.COLUMN_NAME for row in cur_sql.fetchall()]


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
    logger = get_logger("dbt_base_loader", run_environment)
    # print("USE_PROCESSES:", os.getenv("USE_PROCESSES"))

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
        cur_sql.execute(configDB[metadatadb]["sqltablelist"])
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