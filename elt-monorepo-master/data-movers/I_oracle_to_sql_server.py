import tkinter as tk
from tkinter import ttk, messagebox
import sys
import yaml
from pathlib import Path
import threading
import datetime # Import datetime for handling date conversions
import pandas as pd # Import pandas for DataFrame operations

# local packages
from db_utils.env_config import load_environment_dotenv, load_config
from db_utils.logger import get_logger, alert_on_crash
from db_utils.db_connector import get_db_connection

# Target db of the source tables. Used to make the db connection and matches databaseconnections.yaml entry.
targetdb = None  # Will be set by user selection (SQL Server)
sourcedb = None    # Will be set by user selection (Oracle)

# Valid environments for dropdown
VALID_ENVIRONMENTS = ["DEV", "PROD"]

# Initialize shared variables
run_environment = None
env_path = ''
configDB = None
logger = None
db_config = {}  # To store the loaded database configurations
process_thread = None # To hold the running thread
stop_event = threading.Event() # Event to signal the thread to stop

# Function to center the window on the screen
def center_window(root, width=500, height=300):
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    x = (screen_width // 2) - (width // 2)
    y = (screen_height // 2) - (height // 2)
    root.geometry(f"{width}x{height}+{x}+{y}")

# Step 1: Create main window
root = tk.Tk()
root.title("Database Selector")

# Step 2: Set window size and center it
center_window(root, width=400, height=300)

# ============================ Step 3: Environment Selection UI ============================

tk.Label(root, text="Select Environment:").pack(pady=(15, 5))
env_var = tk.StringVar()
env_dropdown = ttk.Combobox(root, textvariable=env_var, values=VALID_ENVIRONMENTS, state="readonly")
env_dropdown.pack(pady=5)

# Placeholders for database selection dropdowns and buttons
source_db_var = tk.StringVar()
source_db_dropdown = ttk.Combobox(root, textvariable=source_db_var, state="disabled")
target_db_var = tk.StringVar()
target_db_dropdown = ttk.Combobox(root, textvariable=target_db_var, state="disabled")
btn_go = tk.Button(root, text="Go", state="disabled")
btn_cancel = tk.Button(root, text="Cancel", command=root.destroy)

def on_env_selected(event=None):
    global run_environment, configDB, logger, db_config, env_path

    selected = env_var.get().upper()
    if selected not in VALID_ENVIRONMENTS:
        messagebox.showerror("Invalid", f"Invalid environment: {selected}")
        return

    run_environment = selected

    try:
        # Load .env
        load_environment_dotenv(run_environment, logger=None)

        # Setup logger
        logger = get_logger("metadata_column_select", run_environment)
        logger.info(f"Environment selected via GUI: {run_environment}")

        # Load config (this should ideally load your databaseconnections.yaml as well)
        # Assuming load_config also returns the path to the environment file, which we use to find the YAML.
        run_environment, configDB, env_file_path = load_config(logger, run_environment)

        env_path = Path(env_file_path) # Store as Path object

        db_yaml_path = env_path.parent / "dataBaseConnections.yaml"

        # Load database configurations from a YAML file
        try:
            with open(db_yaml_path, 'r') as f:
                db_config = yaml.safe_load(f)
        except FileNotFoundError:
            messagebox.showerror("Error", f"dataBaseConnections.yaml not found at {db_yaml_path}. Please ensure it's in the correct directory.")
            sys.exit(1)
        except yaml.YAMLError as e:
            messagebox.showerror("Error", f"Error parsing dataBaseConnections.yaml: {e}")
            sys.exit(1)

        populate_db_dropdowns()
        btn_go.config(state="normal")

    except Exception as e:
        messagebox.showerror("Error", f"Failed to initialize app: {e}")
        sys.exit(1)

# Bind env dropdown to handler
env_dropdown.bind("<<ComboboxSelected>>", on_env_selected)

# ============================ Main UI Database Selection ============================

tk.Label(root, text="Select Source Database (Oracle):").pack(pady=10)
source_db_dropdown.pack(pady=5)

tk.Label(root, text="Select Target Database (SQL Server):").pack(pady=10)
target_db_dropdown.pack(pady=5)

def populate_db_dropdowns():
    oracle_dbs = []
    mssql_dbs = []
    if 'databases' in db_config:
        for db_name, db_info in db_config['databases'].items():
            if db_info.get('type') == 'oracle':
                oracle_dbs.append(db_name)
            elif db_info.get('type') == 'mssql':
                mssql_dbs.append(db_name)

    source_db_dropdown.config(values=oracle_dbs, state="readonly")
    target_db_dropdown.config(values=mssql_dbs, state="readonly")

def on_cancel():
    root.destroy()

def on_go():
    global targetdb, sourcedb
    selected_source_db = source_db_var.get()
    selected_target_db = target_db_var.get()

    if not selected_source_db:
        messagebox.showwarning("No Selection", "Please select a Source Database.")
        return
    if not selected_target_db:
        messagebox.showwarning("No Selection", "Please select a Target Database.")
        return

    sourcedb = selected_source_db  # This will be the Oracle DB
    targetdb = selected_target_db    # This will be the SQL Server DB

    root.withdraw() # Hide the main window
    open_data_window() # Open the data processing window

btn_go.config(command=on_go)
btn_go.pack(pady=5)
btn_cancel.pack(pady=5)

# ============================ Data Processing Functions (Updated) ============================

def run_data_transfer(progress_label, progress_bar, status_text, data_window, close_button, cancel_button):
    """
    Executes the data transfer process in a separate thread.
    Updates the GUI with progress and handles completion/cancellation.
    """
    global sourcedb, targetdb, logger, configDB, stop_event

    connCS = None
    cursorCS = None
    SSconn = None
    SScursor = None

    try:
        progress_label.config(text="Connecting to Oracle Source Database...")
        data_window.update_idletasks()
        connCS = get_db_connection(run_environment, sourcedb, logger)
        cursorCS = connCS.cursor()
        status_text.set("Connected to Oracle Source.")
        data_window.update_idletasks()

        progress_label.config(text="Connecting to SQL Server Target Database...")
        data_window.update_idletasks()
        SSconn = get_db_connection(run_environment, targetdb, logger)
        SScursor = SSconn.cursor()
        status_text.set("Connected to SQL Server Target.")
        data_window.update_idletasks()

        if stop_event.is_set():
            status_text.set("Process cancelled during connection.")
            return

        progress_label.config(text="Fetching data from Oracle...")
        data_window.update_idletasks()
        cursorCS.execute(configDB['source']['sql'])
        columns = [col[0] for col in cursorCS.description]
        resultsCS = cursorCS.fetchall()
        status_text.set(f"{cursorCS.rowcount} Oracle Rows Selected.")
        data_window.update_idletasks()

        if stop_event.is_set():
            status_text.set("Process cancelled during data fetch.")
            return

        progress_label.config(text="Converting data to DataFrame and handling types...")
        data_window.update_idletasks()
        df = pd.DataFrame(resultsCS, columns=columns)

        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]) or (df[col].dtype == 'object' and any(isinstance(x, datetime.datetime) for x in df[col].dropna())):
                df[col] = df[col].apply(
                    lambda x:
                    None if pd.isna(x)
                    else x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                )
            elif df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)

        if stop_event.is_set():
            status_text.set("Process cancelled during data conversion.")
            return

        trunctable = configDB['target']['trunctable']
        truncschema = configDB['target']['truncshema']
        progress_label.config(text=f"Truncating SQL Server table - {truncschema}.{trunctable}")
        data_window.update_idletasks()
        trunctablequery = f"TRUNCATE TABLE [{truncschema}].[{trunctable}]"
        SScursor.execute(trunctablequery)
        status_text.set("Table truncated.")
        data_window.update_idletasks()

        if stop_event.is_set():
            status_text.set("Process cancelled during table truncation.")
            return

        SScursor.fast_executemany = True
        data_tuples = [tuple(x) for x in df.to_numpy()]
        total_rows = len(data_tuples)
        batch_size = 5000
        rowcount = 0

        for i in range(0, total_rows, batch_size):
            if stop_event.is_set():
                status_text.set("Process cancelled during batch insertion. Rolling back...")
                SSconn.rollback() # Attempt to rollback any partial batch
                return

            batch = data_tuples[i:i + batch_size]
            SScursor.executemany(configDB['target']['sqlInsert'], batch)
            rowcount += len(batch)

            progress = min(100, int((rowcount / total_rows) * 100))
            progress_bar['value'] = progress
            status_text.set(f"Inserted {len(batch)} rows... Total: {rowcount}/{total_rows} ({progress}%)")
            data_window.update_idletasks()

        SSconn.commit()
        status_text.set(f"Data transfer complete! Total Rows Inserted: {total_rows}")
        progress_bar['value'] = 100
        logger.info(f"Data transfer from {sourcedb} to {targetdb} completed successfully. Total rows: {total_rows}")

    except Exception as e:
        status_text.set(f"An error occurred: {e}")
        logger.exception("Error during data transfer.")
        if SSconn:
            SSconn.rollback() # Rollback on error
        messagebox.showerror("Error", f"Data transfer failed: {e}")
    finally:
        if cursorCS:
            cursorCS.close()
        if connCS:
            connCS.close()
        if SScursor:
            SScursor.close()
        if SSconn:
            SSconn.close()
        progress_label.config(text="Process Finished.")
        cancel_button.config(state=tk.DISABLED)
        close_button.config(state=tk.NORMAL)
        data_window.update_idletasks()


def open_data_window():
    """
    Opens a new window to display data transfer progress.
    Starts the data transfer in a separate thread.
    """
    data_window = tk.Toplevel(root) # Make it a child window of root
    data_window.title(f"Transferring Data from {sourcedb} to {targetdb}")
    center_window(data_window, width=500, height=250)

    # Variables for dynamic updates
    status_text = tk.StringVar()
    status_text.set("Initializing...")

    progress_label = tk.Label(data_window, text="Starting data transfer...", font=("Arial", 12))
    progress_label.pack(pady=10)

    status_message_label = tk.Label(data_window, textvariable=status_text, font=("Arial", 10), wraplength=480)
    status_message_label.pack(pady=5)

    progress_bar = ttk.Progressbar(data_window, orient="horizontal", length=300, mode="determinate")
    progress_bar.pack(pady=10)

    # Buttons
    close_button = tk.Button(data_window, text="Close", command=lambda: on_transfer_window_close(data_window), state=tk.DISABLED)
    close_button.pack(side=tk.RIGHT, padx=10, pady=10)

    cancel_button = tk.Button(data_window, text="Cancel", command=lambda: on_transfer_cancel(data_window, progress_label, status_text, close_button, cancel_button))
    cancel_button.pack(side=tk.LEFT, padx=10, pady=10)

    # Initialize the stop event
    stop_event.clear()

    # Start the data transfer in a new thread
    global process_thread
    process_thread = threading.Thread(target=run_data_transfer, args=(
        progress_label, progress_bar, status_text, data_window, close_button, cancel_button
    ))
    process_thread.daemon = True # Allow the program to exit even if thread is running
    process_thread.start()

    # Handle window close protocol
    data_window.protocol("WM_DELETE_WINDOW", lambda: on_transfer_window_close(data_window))

def on_transfer_cancel(data_window, progress_label, status_text, close_button, cancel_button):
    """Signals the data transfer thread to stop and updates GUI."""
    if messagebox.askyesno("Cancel Confirmation", "Are you sure you want to cancel the data transfer?"):
        stop_event.set() # Set the event to signal the thread to stop
        status_text.set("Cancellation requested. Waiting for process to stop...")
        cancel_button.config(state=tk.DISABLED) # Disable cancel button immediately
        # The thread's finally block will handle cleanup and enabling close button.

def on_transfer_window_close(data_window):
    """Handles closing the data transfer window and ensures thread cleanup."""
    global process_thread, stop_event
    if process_thread and process_thread.is_alive():
        if messagebox.askyesno("Confirm Exit", "A data transfer is in progress. Do you want to cancel and exit?"):
            stop_event.set()
            # Give the thread a moment to respond to the stop signal
            process_thread.join(timeout=2) # Wait up to 2 seconds for the thread to finish
            if process_thread.is_alive():
                messagebox.showwarning("Warning", "Process did not stop gracefully. Force quitting.")
                sys.exit(1) # Force exit if thread doesn't respond
            data_window.destroy()
            root.destroy()
        else:
            # User chose not to exit, so prevent window from closing
            return
    else:
        # If no thread is running or it has finished
        data_window.destroy()
        root.destroy()


# Start the main app loop
root.mainloop()