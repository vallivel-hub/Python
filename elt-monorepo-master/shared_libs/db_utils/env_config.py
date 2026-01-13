import getpass
import socket
import os
import sys
import logging
import yaml
import inspect

from db_utils.bootstrap_logger import setup_bootstrap_logger
from db_utils.env_utils import resolve_dotenv_path
from db_utils.early_alert import send_early_email_alert
from dotenv import load_dotenv

# --- Constants ---
VALID_ENVIRONMENTS = {"PROD", "DEV"}
logger = setup_bootstrap_logger()

# --- Environment Argument Parsing ---
def parse_run_environment_from_args():
    username = getpass.getuser()
    hostname = socket.gethostname()
    script_name = os.path.basename(sys.argv[0])
    arg_len = len(sys.argv)
    env_arg = sys.argv[1].upper() if arg_len > 1 else None

    if arg_len > 2:
        msg = f"[{script_name}] Too many arguments. Expected one of: 'PROD' or 'DEV'."
    elif not env_arg:
        msg = f"[{script_name}] No arguments entered. Please specify 'PROD' or 'DEV'."
    elif env_arg not in VALID_ENVIRONMENTS:
        msg = f"[{script_name}] Invalid argument '{env_arg}': must be one of {', '.join(VALID_ENVIRONMENTS)}"
    else:
        try:
            dotenv_path = resolve_dotenv_path(env_arg)
            logger.info(
                f"Environment argument validated: {env_arg}",
                extra={"user": username, "host": hostname, "script": script_name}
            )
            return env_arg
        except FileNotFoundError as e:
            msg = str(e)

    # If we hit this point, log & alert the failure
    logger.critical(
        msg,
        extra={"user": username, "host": hostname, "script": script_name}
    )
    send_early_email_alert("Environment Argument Error", f"{msg} (User: {username}, Host: {hostname})")
    raise ValueError(msg)

# --- .env Loader ---
def load_environment_dotenv(run_environment, logger=None):
    """
    Loads the .env file based on the given environment using resolve_dotenv_path().
    Sends early alert if missing.

    Args:
        run_environment (str): The environment name (e.g., DEV, TEST, PROD)
        logger (Logger): Optional logger to use for messages

    Returns:
        Path: The path to the loaded .env file
    """
    if logger is None:
        logger = setup_bootstrap_logger()

    try:
        env_path = resolve_dotenv_path(run_environment)
        load_dotenv(dotenv_path=env_path)
        logger.info(f".env loaded from {env_path}")
        return env_path
    except FileNotFoundError as e:
        msg = str(e)
        logger.critical(msg)
        send_early_email_alert("Missing .env File", msg)
        raise

# --- Caller YAML Config Path ---
def get_caller_conf_path(logger):
    frame = inspect.stack()[2]
    module = inspect.getmodule(frame[0])

    if module and hasattr(module, '__file__'):
        caller_path = os.path.abspath(module.__file__)
        caller_dir = os.path.dirname(caller_path)
        caller_basename = os.path.splitext(os.path.basename(caller_path))[0]
        conf_path = os.path.join(caller_dir, f"{caller_basename}.yaml")

        if not os.path.exists(conf_path):
            msg = f"Caller config file not found at {conf_path}"
            logger.critical(msg)
            raise ValueError(msg)

        logger.debug(f"Caller config path determined: {conf_path}")
        return conf_path

    msg = "Unable to determine caller script path."
    logger.critical(msg)
    raise ValueError(msg)

# --- Main Config Loader ---
def load_config(logger, run_environment):
    logger.info("Beginning config loading...")

    #load_environment_dotenv(run_environment, logger)
    env_path = load_environment_dotenv(run_environment, logger)
    conf_path = get_caller_conf_path(logger)

    try:
        with open(conf_path, "r") as f:
            caller_config = yaml.safe_load(f)
    except FileNotFoundError:
        msg = f"Caller config file not found at {conf_path}"
        logger.critical(msg)
        raise ValueError(msg)
    except yaml.YAMLError as e:
        msg = f"YAML parsing issue in {conf_path}: {e}"
        logger.critical(msg)
        raise ValueError(msg)

    logger.info("Environment and config successfully loaded.")
    return run_environment, caller_config, env_path
