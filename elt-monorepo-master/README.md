<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" class="logo" width="120"/>

## ELT Monorepo Developer Setup Guide

This README provides a comprehensive, step-by-step walkthrough for setting up your local development environment for the `elt-monorepo`. It covers repository structure, environment setup, IDE configuration, and the recommended Git workflow for efficient team collaboration[^1][^2].

### **1. Recommended Base Directory (Cross-IDE Compatible)**

For consistency and ease of use across platforms and IDEs, use a top-level `Projects/` directory for all development repositories:

- **Windows:** `C:/Users/<yourname>/Projects/`
- **macOS/Linux:** `~/Projects/`

**Why?**

- Keeps projects organized and separate from personal files.
- No admin permissions required.
- Works seamlessly with VS Code, IntelliJ IDEA, and other IDEs.
- Simplifies finding virtual environments and Git repositories.

**Avoid using:**
`Desktop/`, `Downloads/`, `Documents/`, `Program Files/` — these are prone to clutter, syncing issues, or permission problems[^1][^2].

### **2. Suggested Monorepo Structure**

After cloning, your directory should look like:

```
Projects/
└── elt-monorepo/
    ├── pscsSourceLoader/    # Primary ETL/ELT project
    ├── shared_libs/         # Shared reusable Python code
    ├── setup/               # Scripts for secure environment setup
    ├── .git/                # Git metadata
    └── ... (other files)
```

**Benefits:**

- Easy to open the entire monorepo in your IDE.
- Simplifies Python interpreter and virtual environment management[^1][^2].


### **3. Initial Setup (First Time Only)**

#### **a. Clone the Repository**

```sh
cd ~/Projects/  # Or C:/Users/<yourname>/Projects/
git clone git@gitlab.its.maine.edu:dsit/python/elt-monorepo.git
cd elt-monorepo
```


#### **b. Set Up Python Virtual Environments**

The monorepo uses dedicated `.venv` directories for isolation. Use the provided setup script:

- **Windows (CMD):**

```sh
.\setup_dev_env.bat
```

- **Windows (PowerShell):**

```sh
.\setup_dev_env.ps1
```

> If you get a script execution warning, run in Administrator PowerShell:
> ```> Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy Bypass -Force >```
- **macOS/Linux:**
> (Follow analogous shell script if provided.)

**What the script does:**

- Creates a `.venv` in `setup/` for utility scripts.
- Installs dependencies from `setup/requirements.txt`.
- Creates a `.venv` in `pscsSourceLoader/` for main development.
- Installs dependencies from `pscsSourceLoader/requirements.txt`.
- Installs `shared_libs` in *editable* mode (`-e ../shared_libs`), so changes are reflected immediately[^1][^2].


#### **c. Secure Environment Setup**

Sensitive configs (database, email) are stored outside Git for security. From the repo root:

```sh
python setup/setup_secure_env.py
```

This creates (or updates) the following in your home directory:

```
~/ELT/
├── .DSITConnections/
│   ├── TEST/
│   │   ├── .env
│   │   └── dataBaseConnections.yaml
│   └── PROD/
│       ├── .env
│       └── dataBaseConnections.yaml
├── .early_alert/
│   └── .env
└── logs/
    └── early_logs/
```

- **Edit these files** with real credentials as needed.
- Files are never committed to Git (see `.gitignore`).
- Script only adds missing files/directories; to reset, delete them manually and rerun[^1][^2].


### **4. Opening the Project in Your IDE**

#### **VS Code**

- Open the entire `elt-monorepo/` folder.
- Use the Command Palette (`Ctrl+Shift+P`), select `Python: Select Interpreter`, and choose the interpreter from `pscsSourceLoader/.venv`.
- Verify shared library imports (e.g., `from db_utils.logger import get_logger`) resolve without errors.


#### **IntelliJ IDEA**

- Open the entire `elt-monorepo/` as a project.
- Set the Project SDK to the Python interpreter in `pscsSourceLoader/.venv`.
- In Project Structure, mark:
    - `pscsSourceLoader/` as a Source Root.
    - `shared_libs/db_utils/` as a Source Root (not the parent `shared_libs/`).
- This enables proper code completion and navigation across the monorepo[^1][^2].


### **5. Git Workflow Quick Start**

#### **a. Create a Feature Branch**

```sh
git checkout -b feature/your-feature-name
```


#### **b. Stay Up-to-Date**

```sh
git checkout master
git pull origin master

# Update your feature branch
git checkout feature/your-feature-name
git merge master
# OR for a cleaner history:
git fetch origin && git rebase origin/master
```


#### **c. Commit and Push**

```sh
git add .
git commit -m "feat: Add new user authentication module"
git push -u origin feature/your-feature-name  # First push
git push  # Subsequent pushes
```


#### **d. Open a Merge Request (MR)**

- On GitLab, go to your project’s Merge Requests.
- Set your feature branch as source, `master` as target, fill in details, and assign reviewers.


#### **e. (Optional) Run Pre-commit Hooks**

```sh
pre-commit run --all-files
```


### **6. Shared Libraries (`shared_libs/`)**

- Changes here affect all dependent projects.
- Test thoroughly before merging.
- For risky changes, use a dedicated feature branch and coordinate with teammates[^1][^2].


## **Summary Table: Common Git Commands**

| Action | Command |
| :-- | :-- |
| Pull latest master | `git pull origin master` |
| Create new branch | `git checkout -b feature/xyz` |
| Add files to staging | `git add .` |
| Commit changes | `git commit -m "Your descriptive message"` |
| Push branch | `git push -u origin feature/xyz` |
| Switch branches | `git checkout master` / `git checkout feature/xyz` |
| Rebase with master | `git fetch origin && git rebase origin/master` |

## **Notes**

- Always open the entire monorepo in your IDE for best results.
- Never commit sensitive files in `~/ELT/`.
- Use feature branches for all changes.
- Keep your branch up-to-date with `master` to avoid conflicts.
- For shared library changes, communicate with your team.

**Happy coding!**
For further questions, consult this README or reach out to your team lead[^1][^2].

<div style="text-align: center">⁂</div>

[^1]: paste.txt

[^2]: paste.txt

