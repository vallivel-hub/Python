"""
AdaptiveScheduler
-----------------
A dynamic concurrency scheduler that executes a list of tasks using either
ThreadPoolExecutor (I/O-bound tasks) or ProcessPoolExecutor (CPU-bound tasks).

Key Features:
- Toggle between thread or process execution via the `use_processes` flag.
- Dynamically adjusts worker pool size based on average task duration.
  - Speeds up when tasks are fast (adds workers up to `max_workers`).
  - Scales down when tasks are slow (down to `min_workers`).
- Collects results in a consistent format: (task_name, elapsed_time, rowcount).
- Logs worker adjustments and task failures if a logger is provided.

Usage:
------
from db_utils.scheduler import AdaptiveScheduler

# Prepare tasks: each task is (task_name, function, args)
tasks = [
    ("table1", my_worker_function, (arg1, arg2)),
    ("table2", my_worker_function, (arg1, arg2)),
]

# Create scheduler (threads by default, processes if use_processes=True)
scheduler = AdaptiveScheduler(max_workers=8, min_workers=2, adjust_interval=60,
                              logger=my_logger, use_processes=True)

# Run tasks
results = scheduler.submit_tasks(tasks)

# Results look like:
# [
#   ("table1", 12.3, 100000),   # task_name, elapsed_seconds, row_count
#   ("table2", 8.7, 50000),
# ]
"""
import time
import threading
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed


class AdaptiveScheduler:
    """
    Adaptive concurrency scheduler.
    Dynamically adjusts worker pool size based on task durations.
    Can run in thread or process mode.
    """

    def __init__(self, max_workers=10, min_workers=2, adjust_interval=60, logger=None, use_processes=False):
        self.max_workers = max_workers
        self.min_workers = min_workers
        self.adjust_interval = adjust_interval
        self.logger = logger
        self.use_processes = use_processes  # <-- toggle for process/thread mode

        # A threading.Lock is only needed in thread mode, but is non-pickleable
        # and MUST be handled by __getstate__ if process mode is used.
        self.lock = threading.Lock()
        self.current_workers = min_workers
        self.last_adjust_time = time.time()
        self.task_durations = []

        mode = "ProcessPoolExecutor" if self.use_processes else "ThreadPoolExecutor"
        if self.logger:
            self.logger.info(f"[Scheduler] Initialized in {mode} mode "
                             f"(min={self.min_workers}, max={self.max_workers})")

    # --- Pickling Methods for Multiprocessing ---

    def __getstate__(self):
        """
        Custom method for pickling.
        Excludes the non-pickleable 'threading.Lock' when using processes.
        """
        state = self.__dict__.copy()

        # If running in process mode, remove the unpickleable lock
        if self.use_processes and 'lock' in state:
            del state['lock']

        return state

    def __setstate__(self, state):
        """
        Custom method for unpickling (in the worker process).
        The lock is not added back as the worker process doesn't need it.
        """
        self.__dict__.update(state)

        # If running in the main process (which uses __init__ and not __setstate__ for the lock)
        # and we need to guarantee it's there (optional, but robust)
        if not self.use_processes and not hasattr(self, 'lock'):
            self.lock = threading.Lock()


    # --- _adjust_workers ---

    def _adjust_workers(self):
        """Adjusts worker count dynamically based on average task duration."""
        now = time.time()
        if now - self.last_adjust_time < self.adjust_interval:
            return

        # self.lock will exist here because this method is called only
        # in the main process where the lock was initialized in __init__
        with self.lock:
            if not self.task_durations:
                return

            avg_duration = sum(self.task_durations) / len(self.task_durations)
            self.task_durations.clear()

            if avg_duration < 10 and self.current_workers < self.max_workers:
                self.current_workers += 1
                if self.logger:
                    self.logger.info(f"[Scheduler] Increasing workers → {self.current_workers}")
            elif avg_duration > 60 and self.current_workers > self.min_workers:
                self.current_workers -= 1
                if self.logger:
                    self.logger.info(f"[Scheduler] Decreasing workers → {self.current_workers}")

            self.last_adjust_time = now

    # --- Other Methods ---

    def submit_tasks(self, tasks):
        """
        Run tasks in parallel with adaptive scheduling.
        Each task is (task_name, func, args).
        Returns [(task_name, elapsed, rowcount), ...]
        """
        results = []
        # Note: self is implicitly pickled when ProcessPoolExecutor is used
        executor_class = ProcessPoolExecutor if self.use_processes else ThreadPoolExecutor

        with executor_class(max_workers=self.max_workers) as executor:
            future_to_task = {
                executor.submit(self._run_task, name, func, args): (name, func)
                for name, func, args in tasks
            }

            for future in as_completed(future_to_task):
                name, func = future_to_task[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"[{name}] Task failed: {e}")
                    results.append((name, 0.0, 0))

                self._adjust_workers() # Called in the main process

        return results

    def _run_task(self, name, func, args):
        """
        Wraps execution to measure elapsed time and enforce uniform result shape.
        Must be pickleable if running in process mode.
        """
        start = time.time()
        # This method runs in the worker process (or worker thread).
        # It does NOT use self.lock, so the missing lock is not an issue.
        try:
            result = func(*args)

            # Normalize outputs
            # ... (rest of normalization logic) ...
            if isinstance(result, tuple) and len(result) == 3:
                task_name, elapsed, rowcount = result
            elif isinstance(result, tuple) and len(result) == 2:
                task_name, rowcount = result
                elapsed = time.time() - start
            else:
                task_name = name
                rowcount = 0
                elapsed = time.time() - start

        except Exception as e:
            if self.logger:
                self.logger.error(f"Tablename - [{name}] scheduler exception in task: {e}")
            task_name = name
            rowcount = 0
            elapsed = time.time() - start

        # Track durations for adaptive scaling (This modifies the worker's COPY of self)
        # When the result is sent back, the main process's copy is the one that matters.
        self.task_durations.append(elapsed)
        return (task_name, elapsed, rowcount)