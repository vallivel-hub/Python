# This is a simple testing program and example on how to use
# Shared utils for logging and emailing for both running locally
# in an IDE and a DAG.

import os
import sys

from dotenv import load_dotenv
from db_utils.db_connector import get_db_connection
from db_utils.env_config import (
    parse_run_environment_from_args,
    load_environment_dotenv,
    load_config,
)
from db_utils.logger import get_logger, alert_on_crash

targetdb = 'UMS_DBT'
targetdblist = 'UMS_DBT'
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

        # Step 6: DB connection
        SSconn = get_db_connection(run_environment, targetdb, logger)
        SScursor = SSconn.cursor()

        # Step 7: Fetch tables
        def get_table_list():
            SScursor.execute(configDB[targetdblist]['sqltablelist'])
            return [row.TABLE_NAME for row in SScursor.fetchall()]

        # Step 8: Loop + log
        tables = get_table_list()
        for table in tables:
            logger.info(f"Processed table: {table}")

        # Cleanup
        SScursor.close()
        SSconn.close()

    _main()

# ========= CLI Entry ==========
if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1])  # Pass CLI argument to main
    else:
        main()  # Fallback to env var or prompt