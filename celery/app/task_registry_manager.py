# -*- coding: utf-8 -*-
"""TaskRegistryManager abstracts management of the application's tasks.

It holds a reference to the application's task registry (a dict-like
registry of task instances) and provides methods to register and
lookup tasks. It can be finalized to prevent further registration.
"""
from celery.app.registry import TaskRegistry


class TaskRegistryManager:
    """Manage addition to and query of the app's task registry."""
    def __init__(self, app: "Celery") -> None:
        self.app = app
        # Use existing task registry if already created
        existing = getattr(app, '_tasks', None)
        if existing is None:
            self.tasks: TaskRegistry = TaskRegistry()
            self.app._tasks = self.tasks
        else:
            self.tasks = existing
        self._finalized = False

    def register_task(self, task):
        """Register a new task with the registry."""
        if self._finalized:
            raise RuntimeError('Task registry has been finalized')
        if task.name in self.tasks:
            raise ValueError(f"Task {task.name!r} already registered")
        self.tasks[task.name] = task
        # bind task to app if not yet bound
        task._app = self.app
        task.bind(self.app)
        return task

    def get_task(self, name: str):
        """Retrieve a task by name or raise KeyError."""
        if name not in self.tasks:
            raise KeyError(name)
        return self.tasks[name]

    def has_task(self, name: str) -> bool:
        """Return True if a task with given name exists."""
        return name in self.tasks

    def finalize(self) -> None:
        """Prevent further tasks from being added."""
        self._finalized = True
