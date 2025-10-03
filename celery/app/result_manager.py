# -*- coding: utf-8 -*-
"""Module containing the ResultManager abstraction.

The ResultManager encapsulates interactions with the configured result
backend, including initialization, storing and retrieving task results.
"""
from celery.app import backends


class ResultManager:
    """Manage result backend lifecycle and result storage."""
    def __init__(self, app: "Celery") -> None:
        self.app = app
        self.backend = None

    def get_backend(self):
        """Return the backend instance or raise if not initialized."""
        if self.backend is None:
            raise RuntimeError('Result backend not initialized')
        return self.backend

    def init_backend(self):
        """Initialize the backend and handle override_backends."""
        if self.backend is None:
            backend_cls, url = backends.by_url(
                self.app.backend_cls or self.app.conf.result_backend,
                self.app.loader,
            )
            # apply any override_backends configured on the loader
            try:
                self.backend = backend_cls(app=self.app, url=url)
            except ImportError:
                raise
        return self.backend

    def store_result(self, task_id, result, state):
        """Store a task result."""
        if self.backend is None:
            raise RuntimeError('Result backend not initialized')
        return self.backend.store_result(task_id, result, state)

    def get_result(self, task_id):
        """Retrieve a task result."""
        if self.backend is None:
            raise RuntimeError('Result backend not initialized')
        return self.backend.get_result(task_id)
