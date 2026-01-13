# db_utils/config_loader.py
import os
import yaml

def get_run_environment(config_path=None, default="TEST"):
    """
    Determines the run environment.

    Priority:
    1. Environment variable RUN_ENV
    2. Config file (YAML)
    3. Default value ("TEST")

    :param config_path: Path to a YAML config file
    :param default: Default environment if none is found
    :return: Environment name as a string
    """
    # Check for environment variable
    env_var = os.getenv("RUN_ENV")
    if env_var:
        return env_var
    
    # Check for config file
    if config_path and os.path.exists(config_path):
        with open(config_path, "r") as configfile:
            config = yaml.safe_load(configfile)
            return config.get("environment", default)
    
    return default

