import tkinter as tk
from tkinter import ttk, messagebox
import sys

# local packages
from db_utils.env_config import load_environment_dotenv, load_config
from db_utils.logger import get_logger, alert_on_crash
from db_utils.db_connector import get_db_connection

# Target db of the source tables. Used to make the db connection and matches databaseconnections.yaml entry.
targetdb = 'UMS_DBT'

# Target db of the oracle_metadata table.  Used to make the db connection and matches databaseconnections.yaml entry.
metadb = 'ELT_MetaData'

# Valid environments for dropdown
VALID_ENVIRONMENTS = ["DEV", "PROD"]

# Initialize shared variables
run_environment = None
configDB = None
logger = None

# Function to center the window on the screen
def center_window(root, width=500, height=300):
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    x = (screen_width // 2) - (width // 2)
    y = (screen_height // 2) - (height // 2)
    root.geometry(f"{width}x{height}+{x}+{y}")

# Step 1: Create main window
root = tk.Tk()
root.title("SQL Server Table Viewer")

# Step 2: Set window size and center it
center_window(root, width=400, height=250)

# ============================ Step 3: Environment Selection UI ============================

tk.Label(root, text="Select Environment:").pack(pady=(15, 5))
env_var = tk.StringVar()
env_dropdown = ttk.Combobox(root, textvariable=env_var, values=VALID_ENVIRONMENTS, state="readonly")
env_dropdown.pack(pady=5)

# Placeholder until after environment is set
table_var = tk.StringVar()
table_dropdown = ttk.Combobox(root, textvariable=table_var, state="disabled")
btn_go = tk.Button(root, text="Go", state="disabled")
btn_cancel = tk.Button(root, text="Cancel", command=root.destroy)

def on_env_selected(event=None):
    global run_environment, configDB, logger

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

        # Load config
        run_environment, configDB, _ = load_config(logger, run_environment)

        # Populate table names
        table_dropdown.config(values=get_table_names(), state="readonly")
        btn_go.config(state="normal")

    except Exception as e:
        messagebox.showerror("Error", f"Failed to initialize app: {e}")
        sys.exit(1)

# Bind env dropdown to handler
env_dropdown.bind("<<ComboboxSelected>>", on_env_selected)

# ============================ Main UI Table Selection ============================

tk.Label(root, text="Select Table:").pack(pady=10)
table_dropdown.pack(pady=5)

def on_cancel():
    root.destroy()

def on_go():
    selected_table = table_var.get()
    if selected_table:
        root.withdraw()
        open_data_window(selected_table)
    else:
        messagebox.showwarning("No Selection", "Please select a table")

btn_go.config(command=on_go)
btn_go.pack(pady=5)
btn_cancel.pack(pady=5)

# ============================ Supporting Functions ============================

def get_table_names():
    try:
        conn = get_db_connection(run_environment, targetdb, logger)
        cursor = conn.cursor()
        cursor.execute(configDB[targetdb]['sqltablelist'])
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables
    except Exception as e:
        logger.exception("Error fetching table list")
        messagebox.showerror("Database Error", f"Error fetching table list: {e}")
        root.destroy()

def fetch_table_data(table_name):
    try:
        conn = get_db_connection(run_environment, metadb, logger)
        cursor = conn.cursor()
        cursor.execute(configDB[metadb]['sqlfetchtable'], table_name)
        data = [(row[0], row[1], row[2]) for row in cursor.fetchall()]
        conn.close()
        return data
    except Exception as e:
        logger.exception(f"Error fetching data for {table_name}")
        messagebox.showerror("Database Error", f"Could not fetch table data: {e}")
        return []

def get_dim1_value(table_name):
    conn = get_db_connection(run_environment, metadb, logger)
    cursor = conn.cursor()
    cursor.execute(configDB[metadb]['sqlgetdim'], table_name)
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def update_table(table_name, updated_data):
    conn = get_db_connection(run_environment, metadb, logger)
    cursor = conn.cursor()
    for col1, col2, col3 in updated_data:
        use_column = 'Y' if col3 else None
        cursor.execute(configDB[metadb]['sqlupdateusecolumn'], use_column, col1, col2)
    conn.commit()
    conn.close()
    logger.status_email(f"📧 Table, {table_name}, has had column selections changed in oracle_metadata.")

def update_table_to_dim(table_name):
    conn = get_db_connection(run_environment, metadb, logger)
    cursor = conn.cursor()
    cursor.execute(configDB[metadb]['sqlupdatedimension'], ('Y', table_name))
    conn.commit()
    conn.close()
    logger.status_email(f"📧 Table, {table_name}, has been flagged to be a quick dimension view in oracle_metadata.")

def undo_update_table_to_dim(table_name):
    conn = get_db_connection(run_environment, metadb, logger)
    cursor = conn.cursor()
    cursor.execute(configDB[metadb]['sqlupdatedimension'], (None, table_name))
    conn.commit()
    conn.close()
    logger.status_email(f"📧 Table, {table_name}, has been UN-flagged to be a quick dimension view in oracle_metadata. Manually drop view in database.")

def open_data_window(table_name):
    data_window = tk.Toplevel()
    data_window.title(f"Editing {table_name}")
    center_window(data_window, width=450, height=800)

    def on_close():
        if messagebox.askyesno("Exit", "Are you sure you want to close the application?"):
            root.destroy()

    data_window.protocol("WM_DELETE_WINDOW", on_close)

    frame = tk.Frame(data_window)
    frame.pack(fill=tk.BOTH, expand=True)

    canvas = tk.Canvas(frame)
    scrollbar = tk.Scrollbar(frame, orient=tk.VERTICAL, command=canvas.yview)
    scrollable_frame = tk.Frame(canvas)

    scrollable_frame.bind(
        "<Configure>",
        lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
    )

    canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
    canvas.configure(yscrollcommand=scrollbar.set)

    canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def on_mouse_wheel(event):
        canvas.yview_scroll(-1 * (event.delta // 120), "units")

    data_window.bind_all("<MouseWheel>", on_mouse_wheel)

    data = fetch_table_data(table_name)
    check_vars = []

    tk.Label(scrollable_frame, text="Columns", font=("Arial", 15, "bold")).grid(row=0, column=0, padx=5, pady=5)
    tk.Label(scrollable_frame, text="Unique", font=("Arial", 15, "bold")).grid(row=0, column=1, padx=5, pady=5)
    tk.Label(scrollable_frame, text="Want", font=("Arial", 15, "bold")).grid(row=0, column=2, padx=5, pady=5)

    for i, (col1, col2, col3) in enumerate(data, start=1):
        tk.Label(scrollable_frame, text=col1).grid(row=i, column=0, padx=5, pady=2)
        tk.Label(scrollable_frame, text=col2).grid(row=i, column=1, padx=5, pady=2)

        var = tk.BooleanVar(value=(col3 == 'Y'))
        state = tk.DISABLED if col2 == 'Y' else tk.NORMAL
        chk = tk.Checkbutton(scrollable_frame, variable=var, state=state)
        chk.grid(row=i, column=2, padx=5, pady=2)
        check_vars.append((col1, col2, var))

    def update_data():
        updated_data = [(table_name, col1, var.get()) for col1, col2, var in check_vars]
        update_table(table_name, updated_data)
        messagebox.showinfo("Success", "Table updated successfully")
        data_window.destroy()
        root.destroy()

    def update_dim_data():
        update_table_to_dim(table_name)
        messagebox.showinfo("Success", "Table flagged as dimensional")

    def undo_update_dim_data():
        undo_update_table_to_dim(table_name)
        messagebox.showinfo("Success", "Table unflagged as dimensional — remember to manually delete view in DB")
        data_window.destroy()
        root.destroy()

    def cancel():
        root.destroy()

    dim1_value = get_dim1_value(table_name)

    btn_update = tk.Button(scrollable_frame, text="Update", command=update_data)
    btn_dim_update = tk.Button(scrollable_frame, text="UpdateToDIM", command=update_dim_data)
    btn_cancel = tk.Button(scrollable_frame, text="Cancel", command=cancel)
    btn_undo = tk.Button(scrollable_frame, text="Undo_UpdateToDIM", command=undo_update_dim_data)

    if dim1_value == 'Y':
        btn_dim_update.config(state=tk.DISABLED)
    else:
        btn_undo.config(state=tk.DISABLED)

    btn_update.grid(row=len(data)+1, column=1, padx=10, pady=10)
    btn_dim_update.grid(row=len(data)+1, column=2, padx=10, pady=10)
    btn_cancel.grid(row=len(data)+1, column=3, padx=10, pady=10)
    btn_undo.grid(row=len(data)+2, column=2, padx=10, pady=10)

# Start the main app loop
root.mainloop()
