import os
import yaml
import oracledb
import pyodbc
import sys

def load_db_config(run_env):
    """Load database configurations from a YAML file."""
    home_dir = os.path.expanduser("~")
    conf_path = os.path.join(home_dir, "ELT", ".DSITconnections", run_env, "dataBaseConnections.yaml")

    try:
        with open(conf_path, "r") as config_file:
            config = yaml.safe_load(config_file)
        return config.get('databases', {})
    except FileNotFoundError:
        print(f"Error: Config file not found at {conf_path}")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")
        sys.exit(1)

def get_db_connection(env, db_name):
    """Establish a database connection based on the configuration."""
    config_db = load_db_config(env)

    if db_name not in config_db:
        raise ValueError(f"Database '{db_name}' not found in configuration.")

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
            connection_string = (
                f"DRIVER={db_config['driver']};"
                f"SERVER={db_config['server']};"
                f"DATABASE={db_config['database']};"
                f"UID={db_config['user']};"
                f"PWD={db_config['password']}"
            )
            connection = pyodbc.connect(connection_string)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

        return connection

    except Exception as e:
        print(f"Error connecting to {db_name}: {e}")
        sys.exit(1)