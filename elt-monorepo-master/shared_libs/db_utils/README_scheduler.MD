# Scheduler Best Practices

1. **Thread vs Process Mode**

    * **Thread mode (`use_processes=False`)**

        * Best when tasks are I/O-bound (waiting on database, network, or disk).

        * Lighter weight, faster startup, avoids pickling overhead.

    * **Process mode (`use_processes=True`)**

        * Best when tasks are CPU-bound (heavy computation, large data transformations).

        * More memory-intensive, but avoids GIL bottlenecks.

    * 👉 **Try both modes in staging before committing to production.**

2. **Worker Tuning**

    * `min_workers`: Set low enough to avoid resource starvation (2–4 is typical).

    * `max_workers`: Match to number of CPU cores (process mode) or expected concurrent tasks (thread mode).

    * `adjust_interval`: Increase if tasks are long-running (>5 min) to avoid noisy scaling.

3. **Error Handling**

    * Each task is wrapped safely; failures return (`task_name`, `0.0`, `0`).

    * Make sure your logger captures `[Scheduler]` messages.

    * If a task must be retried, implement retry logic in the task function itself, not the scheduler.

4. **Monitoring**

    * Log task start/end, elapsed time, and row counts.

    * Track `avg_duration` and worker scaling decisions in logs.

    * Consider emitting metrics to Prometheus/Grafana for visibility.

5. **When to Batch**

    * Consider batching tables if:

        * You have hundreds/thousands of very small tasks.

        * Overhead of task submission > task runtime.

    * Keep current design for medium-to-large table loads.