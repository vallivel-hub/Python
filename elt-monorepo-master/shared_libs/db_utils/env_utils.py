import os
from dotenv import load_dotenv
from db_utils.bootstrap_logger import setup_bootstrap_logger
from db_utils.early_alert import send_early_email_alert
from functools import lru_cache

@lru_cache()  # cache path results for performance
def resolve_dotenv_path(run_env: str) -> str:
    """
    Resolves the .env path depending on whether we're running in Docker (Airflow) or locally.
    - In Docker: uses a mounted path like /opt/dsit_connections/{ENV}/.env
    - Locally: resolves to ~/ELT/.DSITconnections/{ENV}/.env

    Args:
        run_env: Environment name, e.g. 'DEV', 'PROD', or 'AIRFLOW'.

    Returns:
        Absolute path to the resolved .env file.

    Raises:
        FileNotFoundError if no valid .env path is found.
    """
    docker_path = f"/opt/dsit_connections/{run_env}/.env"
    local_path = f"C:/ELT/.DSITconnections/{run_env}/.env"

    if os.path.exists(docker_path):
        return os.path.abspath(docker_path)
    elif os.path.exists(local_path):
        return local_path
    else:
        raise FileNotFoundError(
            f".env file not found for run_env='{run_env}'. Tried:\n"
            f"  - Docker path: {docker_path}\n"
            f"  - Local path:  {local_path}"
        )

def load_environment_dotenv(run_environment: str, logger=None):
    """
    Loads the .env file for the given environment. Uses resolve_dotenv_path to support Airflow & local use.
    Sends early alert if the file is missing.
    """
    if logger is None:
        logger = setup_bootstrap_logger()

    try:
        env_path = resolve_dotenv_path(run_environment)
        load_dotenv(dotenv_path=env_path)
        logger.info(f".env loaded from {env_path}")
    except FileNotFoundError as e:
        msg = str(e)
        logger.critical(msg)
        send_early_email_alert("Missing .env File", msg)
        raise

def resolve_dbconfig_path(run_env: str) -> str:
    """
    Resolves the path to the dataBaseConnections.yaml file for the given environment.
    Supports Airflow and local use.
    """
    docker_path = f"/opt/dsit_connections/{run_env}/dataBaseConnections.yaml"
    home_path = f"C:/ELT/.DSITconnections/{run_env}/dataBaseConnections.yaml"

    if os.path.exists(docker_path):
        return os.path.abspath(docker_path)
    elif os.path.exists(home_path):
        return home_path
    else:
        raise FileNotFoundError(
            f"DB config file not found. Checked:\n  - {docker_path}\n  - {home_path}"
        )