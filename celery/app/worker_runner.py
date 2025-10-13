# -*- coding: utf-8 -*-
"""WorkerRunner abstracts the worker lifecycle management for a Celery app.

It exposes simple hooks to initialize the worker state, shut it down
and prepare per-process resources prior to task execution. This API
wraps the corresponding hooks on the configured loader.
"""


class WorkerRunner:
    """Manage worker lifecycle hooks for the application."""
    def __init__(self, app: "Celery", loader) -> None:
        self.app = app
        self.loader = loader
        self._initialized = False

    def init_worker(self) -> None:
        """Initialize the worker and its resources."""
        if not self._initialized:
            self.loader.init_worker()
            self._initialized = True

    def shutdown_worker(self) -> None:
        """Clean up the worker and resources gracefully."""
        self.loader.shutdown_worker()

    def init_worker_process(self) -> None:
        """Prepare pre-process resources before task execution."""
        self.loader.init_worker_process()
