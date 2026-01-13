# SamplePolarsLoad Orchestration

This script coordinates Oracle → SQL Server table loads using Polars, with adaptive scheduling.

---

## Features
- Pulls table metadata from SQL Server
- Retrieves row counts from Oracle for sorting
- Prioritizes large tables first
- AdaptiveScheduler manages concurrency (thread or process mode)
- Logs per-table runtime and summary

---

## Environment Variables

DEV or PROD passed as arg1

Set these in your `.env` file (or environment):

```ini
RUN=DEV
LOG_LEVEL=DEBUG
USE_PROCESSES=1   # 1 = use ProcessPoolExecutor, 0 = use ThreadPoolExecutor
EMAIL_ALERTS=true
SMTP_HOST=smarthost.
SMTP_PORT=465
LOG_FROM_EMAIL=sender1@me.com
LOG_ADMIN_EMAIL=mimi@me.com # use "mi@me.com,mi2@me.com"  for multiple emails
```

---

## Running

```bash
python SamplePolarsLoad.py DEV
```

- If `USE_PROCESSES=1`, runs with `ProcessPoolExecutor`
- If `USE_PROCESSES=0`, runs with `ThreadPoolExecutor`

---

## Example Log Output

```
USE_PROCESSES: 1
[2025-09-29 15:26:03] [INFO] [OracleToSQLServer] [env=DEV program=SamplePolarsLoad.py] [Startup] USE_PROCESSES=True → ProcessPoolExecutor
[2025-09-29 15:26:03] [INFO] [__main__] [env=DEV program=SamplePolarsLoad.py] ▶️ Starting `_main`
```

---

## Module Relationships

Below is a diagram of how the modules and components fit together:

```
               ┌───────────────────────────┐
               │   SamplePolarsLoad.py     │
               │  (main orchestrator)      │
               └─────────────┬─────────────┘
                             │
           ┌─────────────────┼──────────────────┐
           │                 │                  │
┌────────────────┐   ┌─────────────────┐   ┌──────────────────────┐
│  load_config   │   │  get_db_conn    │   │  AdaptiveScheduler    │
│ (YAML/env)     │   │ (Oracle/SQL)    │   │ (threads/processes)   │
└────────────────┘   └─────────────────┘   └──────────────────────┘
           │                 │
           ▼                 ▼
   ┌─────────────┐     ┌───────────────┐
   │  SQL Server │     │    Oracle     │
   │ table list  │     │ row counts    │
   └─────────────┘     └───────────────┘
                             │
                             ▼
                  ┌────────────────────┐
                  │   process_table    │
                  │ (per-table loader) │
                  └────────────────────┘
```

---
