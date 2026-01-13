import os
import yaml
import oracledb
import pyodbc

from db_utils.env_utils import resolve_dbconfig_path  # NEW utility to centralize path
from db_utils.early_alert import send_early_email_alert

def load_db_config(run_env, logger):
    """
    Load database configurations from a YAML file for the specified environment.
    Uses resolve_dbconfig_path() to support both local and Airflow paths.
    """

    try:
        config_path = resolve_dbconfig_path(run_env)
        with open(config_path, "r") as config_file:
            config = yaml.safe_load(config_file)
        logger.info(f"Loaded DB config from: {config_path}")
        return config.get("databases", {})
    except Exception as e:
        msg = f"DB config load error for run_env={run_env}: {e}"
        if logger:
            logger.critical(msg)
        send_early_email_alert("Missing DB Config", msg)
        raise

def get_db_connection(run_env, db_name, logger):
    """
    Establish a database connection based on the given environment and database name.
    Supports Oracle and MSSQL.
    """
    logger.debug(f"Getting DB connection for env={run_env}, db_name={db_name}")
    config_db = load_db_config(run_env, logger)

    if db_name not in config_db:
        msg = f"Database '{db_name}' not found in configuration."
        logger.critical(msg)
        raise ValueError(msg)

    db_config = config_db[db_name]
    db_type = db_config.get('type')

    try:
        if db_type == 'oracle':
            connection = oracledb.connect(
                user=db_config['user'],
                password=db_config['password'],
                dsn=db_config['dsn']
            )
        elif db_type == 'mssql':
            # Add KeepAlive to preventing firewalls from killing "silent" long-running queries
            connection_string = (
                f"DRIVER={db_config['driver']};"
                f"SERVER={db_config['server']};"
                f"DATABASE={db_config['database']};"
                f"UID={db_config['user']};"
                f"PWD={db_config['password']};"
                "KeepAlive=30"  # <-- CRITICAL ADDITION: Send heartbeat every 30 seconds
            )
            connection = pyodbc.connect(connection_string)
        else:
            msg = f"Unsupported database type: {db_type}"
            logger.critical(msg)
            raise ValueError(msg)

        logger.info(f"Successfully connected to {db_name} ({db_type})")
        return connection

    except Exception as e:
        msg = f"Error connecting to {db_name}: {e}"
        logger.critical(msg)
        raise ValueError(msg)