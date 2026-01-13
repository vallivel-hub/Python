#
# This is the first program run in the process of pscsSourceLoader.  It is used to find tables in the MetaData.DataObject table
# that are marked as valid to pull data from an oracle source to the SQL Server database.
# Then pull metadata about the "twin" tables in the oracle source database and adds that data to
# [ELT_MetaData].[MetaData].[oracle_metadata] to store table_name, column_name, column information and creates
# a hashkey code using column_names, uniqueness, and column_id (for proper order) in column_id = 1 of each table.
# This hash key can be used to standardize and simplify table key creation for the new tables.
#
# NOTE: The SQL in the yaml file merges new rows so old rows aren't touched when this is run.  To remove a table entry
# you must run sql to delete the table_name from oracle_metadata manually.  Other modifications are done via 01_oracle_metadata_column_selector.py
#
# THIS SCRIPT DOES NOT CREATE THE [ELT_MetaData].[MetaData].[oracle_metadata] TABLE  See DDL in yaml file.

import os
import sys
import oracledb

from db_utils.db_connector import get_db_connection
from db_utils.env_config import (
    parse_run_environment_from_args,
    load_environment_dotenv,
    load_config,
)

from db_utils.logger import get_logger, log_this, alert_on_crash

# -------- Target/source databases --------
# These are the tags in the dbconnections.yaml file used to connect to the appropriate database servers.
targetdb = 'UMS_DBT'
sourcedb = 'CSPRD'

# --------
# These are the tags in the yaml file used to pull the appropriate SQL.
targetdbsql = 'ELT_MetaData'

def main(run_environment=None):
    # Step 1: Resolve run environment (CLI arg > env var > fallback)
    run_environment = (
            run_environment
            or os.getenv("RUN_ENV")
            or parse_run_environment_from_args()
    )

    # Step 2: Load .env early (needed for logger setup)
    load_environment_dotenv(run_environment, logger=None)

    # Step 3: Initialize logger
    logger = get_logger("EmailTest", run_environment)

    # Step 4: Now wrap actual logic with alert-on-crash
    @alert_on_crash(logger)
    def _main():
        # Step 5: Load config
        _, configDB, _ = load_config(logger, run_environment)

        # Step 5: DB connections
        connectionCS = get_db_connection(run_environment, sourcedb, logger)
        cursorCS = connectionCS.cursor()

        SSconn = get_db_connection(run_environment, targetdb, logger)
        SScursor = SSconn.cursor()

        # ===== Get Table names to loop through
        def get_table_list():
            SScursor.execute(configDB[targetdbsql]['sqltablelist'])
            return [row.TABLE_NAME for row in SScursor.fetchall()]

        def get_unique_columns_for_table(table_name):
            SScursor.execute(configDB[targetdbsql]['sqluniquecolumns'], table_name)
            return [row.COLUMN_NAME for row in SScursor.fetchall()]

        def load_oracle_metadata(table_name):
            try:
                cursorCS.execute(configDB[sourcedb]['sql'], (table_name, table_name))
                resultscs = cursorCS.fetchall()
            except oracledb.Error as error:
                logger.critical(f"Oracle Error: {error}")
                return

            SScursor.fast_executemany = True
            SScursor.executemany(configDB[targetdbsql]['sqlInsertOracleMetadata'], resultscs)
            SScursor.commit()
            logger.debug("Rows Inserted")

        def create_and_load_hashkeys(table_name, unique_columns):
            """
            Builds and loads both SQL Server and Oracle hashkey expressions
            into the oracle_metadata table for a given table.

            - SQL Server expression uses HASHBYTES('MD5', UPPER(CONCAT(...))) -> CONVERT(..., 2) -> UPPER(...)
            - Oracle expression uses STANDARD_HASH(UPPER(<concatenated cols>), 'MD5') -> RAWTOHEX -> UPPER(...)
            """
            if not unique_columns:
                logger.warning(f"No unique columns found for {table_name}; skipping hashkey generation.")
                return

            delimiter = "###"

            # SQL Server formatters (safe trimming & coalescing)
            unique_columns_sql = [f"LTRIM(RTRIM(COALESCE({col}, '')))" for col in unique_columns]

            # Oracle formatters (TRIM + NVL)
            unique_columns_oracle = [f"TRIM(NVL({col}, ''))" for col in unique_columns]

            # Build expressions
            if len(unique_columns) == 1:
                # Single column — simpler expressions
                sqlserver_expr = (
                    f"[Hash_{table_name}_Key] = "
                    f"UPPER(CONVERT(CHAR(32), HASHBYTES('MD5', UPPER({unique_columns_sql[0]})), 2))"
                )

                oracle_expr = (
                    f"UPPER(RAWTOHEX(STANDARD_HASH(UPPER({unique_columns_oracle[0]}), 'MD5'))) "
                    f'AS "Hash_{table_name}_Key"'
                )
            else:
                # Multiple columns — join with delimiter
                concat_sqlserver = f", '{delimiter}', ".join(unique_columns_sql)
                # For Oracle use the || operator between trimmed NVL expressions
                concat_oracle = f" || '{delimiter}' || ".join(unique_columns_oracle)

                # SQL Server: ensure proper parentheses so CONVERT(..., 2) wraps HASHBYTES result
                sqlserver_expr = (
                    f"[Hash_{table_name}_Key] = "
                    f"UPPER(CONVERT(CHAR(32), HASHBYTES('MD5', UPPER(CONCAT({concat_sqlserver}))), 2))"
                )

                # Oracle: don't wrap the ||-joined expression in CONCAT; just pass the full concatenation
                oracle_expr = (
                    f"UPPER(RAWTOHEX(STANDARD_HASH(UPPER({concat_oracle}), 'MD5'))) "
                    f'AS "Hash_{table_name}_Key"'
                )

            # Log both expressions for visibility
            logger.debug(f"[{table_name}] SQL Server hash expression: {sqlserver_expr}")
            logger.debug(f"[{table_name}] Oracle hash expression: {oracle_expr}")

            # Update metadata table: expects sqlupdatehashkeys to accept (sqlserver_expr, oracle_expr, table_name)
            query = configDB[targetdbsql]['sqlupdatehashkeys']

            try:
                SScursor.execute(query, (sqlserver_expr, oracle_expr, table_name))
                SSconn.commit()
                logger.info(f"Hashkey expressions updated for {table_name}.")
            except Exception as e:
                logger.error(f"Failed to update hashkey expressions for {table_name}: {e}")


        # ===== Main Loop =====
        tables = get_table_list()
        for table in tables:
            load_oracle_metadata(table)
            unique_columns = get_unique_columns_for_table(table)
            create_and_load_hashkeys(table, unique_columns)
            logger.info(f"Processed table: {table}")

        # ===== Cleanup =====
        cursorCS.close()
        SScursor.close()
        SSconn.close()
        connectionCS.close()

    _main()

# ========= CLI Entry ==========
if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1])  # Pass CLI argument to main
    else:
        main()  # Fallback to env var or prompt