from pathlib import Path

# === Local dev base directory for project data ===
ELT_BASE = Path.home() / "ELT"

# === Common Local-only Paths ===
LOGS_PATH = ELT_BASE / "logs"
METADATA_PATH = ELT_BASE / "metadata"
EXPORTS_PATH = ELT_BASE / "exports"
SCD_CONFIG_PATH = ELT_BASE / "scd_configs"

# === Example function: get path to a specific SCD YAML config ===
def get_scd_config_path(table_name: str) -> Path:
    """
    Returns the full path to a specific SCD config YAML file for a given table.
    """
    return SCD_CONFIG_PATH / f"{table_name}.yaml"

# === Optional: Developer-only scratch space ===
SCRATCH_PATH = ELT_BASE / "scratch"

# === Optional: default for saving test results ===
RESULTS_PATH = ELT_BASE / "results"
