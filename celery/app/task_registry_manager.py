# -*- coding: utf-8 -*-
"""Task registry management adhering to SRP."""
from __future__ import annotations

from contextlib import contextmanager

from celery.app.registry import TaskRegistry


class TaskRegistryManager:
    """Manage addition to and lookup of application tasks."""

    def __init__(self, app: "Celery") -> None:
        self.app = app
        existing = getattr(app, '_tasks', None)
        if existing is None:
            self.tasks: TaskRegistry = TaskRegistry()
            self.app._tasks = self.tasks
        else:
            self.tasks = existing
        self._finalized = False

    def _within_finalization(self) -> bool:
        mutex = getattr(self.app, '_finalize_mutex', None)
        if mutex is None:
            return False
        owned = getattr(mutex, '_is_owned', None)
        if owned is None:
            return False
        return bool(owned())

    def _ensure_can_register(self) -> None:
        if self._finalized and not self._within_finalization():
            raise RuntimeError('Task registry has been finalized')

    def _maybe_autofinalize(self) -> None:
        if getattr(self.app, 'autofinalize', False) and not self.app.finalized:
            self.app.finalize(auto=True)

    @property
    def finalized(self) -> bool:
        return self._finalized

    @contextmanager
    def allow_registration(self):
        was_finalized = self._finalized
        if was_finalized:
            self._finalized = False
        try:
            yield
        finally:
            if was_finalized:
                self._finalized = True

    def register_task(self, task):
        """Register a new task with the registry."""
        self._ensure_can_register()
        if task.name in self.tasks:
            raise ValueError(f"Task {task.name!r} is already registered")
        self.tasks[task.name] = task
        task._app = self.app
        task.bind(self.app)
        return task

    def get_task(self, name: str):
        """Retrieve a task by name or raise :class:`KeyError`."""
        if name not in self.tasks:
            self._maybe_autofinalize()
        if name not in self.tasks:
            raise KeyError(name)
        return self.tasks[name]

    def has_task(self, name: str) -> bool:
        """Return ``True`` if a task with the given name exists."""
        if name in self.tasks:
            return True
        self._maybe_autofinalize()
        return name in self.tasks

    def finalize(self) -> None:
        """Prevent further tasks from being added."""
        self._finalized = True

    def reopen(self) -> None:
        """Allow task registration after a finalize event."""
        self._finalized = False
