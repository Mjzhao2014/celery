# -*- coding: utf-8 -*-
"""High level managers components for the Celery application.

This module provides auxiliary manager classes that encapsulate the
different cross-cutting responsibilities of the Celery application such
as configuration handling, task registry management, result backend
handling, signal dispatch, and worker lifecycle management.

All managers are designed to be instantiated from a :class:`~celery.Celery`
application instance and expose a unit-testable interface following the
single responsibility principle.
"""
from __future__ import annotations

import typing
from types import ModuleType
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Union

from celery.utils.dispatch import Signal

if typing.TYPE_CHECKING:  # pragma: no cover  # pylint: disable=unused-import
    from celery import Celery

__all__ = (
    'SignalManager',
    'ConfigurationManager',
    'TaskRegistryManager',
    'ResultManager',
    'WorkerRunner',
)


class SignalManager:
    """Manage Celery and user-defined signal objects."""
    def __init__(self, app: Celery) -> None:
        # Delay import to avoid circular import.
        self.app: Celery = app
        # holds both built-in and custom signals by name
        self._signals: Dict[str, Signal] = {}
        self._builtin_names: set[str] = set()
        self._init_builtins()

    def _init_builtins(self) -> None:
        """Populate the internal mapping of builtin signals."""
        # built-in module-level signals:
        import celery.signals as builtin_signals
        for name, maybe_sig in vars(builtin_signals).items():
            if isinstance(maybe_sig, Signal):
                self._signals[name] = maybe_sig
        # app-level signals:
        for name in ('on_configure', 'on_after_configure',
                     'on_after_finalize', 'on_after_fork'):
            sig: Optional[Signal] = getattr(self.app, name, None)
            if isinstance(sig, Signal):
                self._signals[name] = sig
        self._builtin_names = set(self._signals.keys())

    def connect(self, signal: Signal, handler: Callable, **kwargs: Any) -> None:
        """Connect a handler to the given signal."""
        signal.connect(handler, **kwargs)

    def disconnect(self, signal: Union[Signal, str], handler: Callable) -> None:
        """Disconnect a handler from the given signal."""
        sig = self._resolve(signal)
        sig.disconnect(handler)

    def emit(self, signal: Signal, **kwargs: Any) -> None:
        """Emit a signal with provided arguments."""
        signal.send(sender=self.app, **kwargs)

    def register_signal(self, name: str) -> Signal:
        """Register a new custom signal by name."""
        if name in self._signals:
            raise ValueError(f'Signal {name!r} already exists')
        sig = Signal(name=name)
        self._signals[name] = sig
        return sig

    def remove_signal(self, signal: Union[Signal, str]) -> None:
        """Remove a custom signal."""
        name = signal if isinstance(signal, str) else signal.name
        if name in self._builtin_names:
            raise KeyError(f'Cannot remove built-in signal {name!r}')
        if name not in self._signals:
            raise KeyError(f'Signal {name!r} not found')
        del self._signals[name]

    def get_signal(self, name: str) -> Signal:
        """Return signal by name."""
        if name not in self._signals:
            raise KeyError(name)
        return self._signals[name]

    def has_signal(self, name: str) -> bool:
        """Return True if a signal with the given name exists."""
        return name in self._signals

    def _resolve(self, signal: Union[Signal, str]) -> Signal:
        return signal if isinstance(signal, Signal) else self.get_signal(signal)


class ConfigurationManager:
    """Encapsulate application configuration loading and parsing."""
    def __init__(self, app: Celery) -> None:
        self.app: Celery = app
        # reference to override_backends used by result backends.
        self.override_backends: Dict[str, str] = {}
        # ensure loader is initialized lazily like in app
        self.loader = self.app.loader
        self.conf = self.app.conf  # bind to app conf object

    def config_from_object(self, obj: Union[Mapping, ModuleType, str, object], silent: bool = False) -> Mapping:
        """Apply configuration from an object or module."""
        if isinstance(obj, str):
            # interpret string as module path/name
            # Propagate ImportError if the module cannot be imported.
            self.loader.config_from_object(obj, silent=silent)
        elif isinstance(obj, Mapping):
            # granular dict applies directly to conf.
            self.app.conf.update(obj)  # type: ignore
        else:
            # object or module:
            self.loader.config_from_object(obj, silent=silent)
        # update tracking of any override_backends provided by loader.
        self.override_backends = getattr(self.loader, 'override_backends', {})
        return self.app.conf

    def read_configuration(self, env: str = 'CELERY_CONFIG_MODULE') -> Optional[Mapping]:
        """Read configuration from environment variable."""
        return self.loader.read_configuration(env)

    def cmdline_config_parser(self, args: Sequence[str], namespace: str = 'celery', **kwargs: Any) -> Dict[str, Any]:
        """Parse command-line arguments as configuration."""
        return self.loader.cmdline_config_parser(args, namespace=namespace, **kwargs)


class TaskRegistryManager:
    """Manage application task registry."""
    def __init__(self, app: Celery) -> None:
        self.app: Celery = app
        # underlying dict of tasks
        self.tasks: Dict[str, Any] = app._tasks
        self._finalized: bool = False

    def register_task(self, task: Any) -> Any:
        """Add task instance to registry."""
        if self._finalized:
            raise RuntimeError('Cannot register tasks after app finalized')
        if task.name in self.tasks:
            raise ValueError(f'Task {task.name!r} already registered')
        self.tasks[task.name] = task
        return task

    def get_task(self, name: str) -> Any:
        """Get a task by name."""
        try:
            return self.tasks[name]
        except KeyError:
            raise KeyError(f'Task {name!r} not found')

    def has_task(self, name: str) -> bool:
        """Return True if task exists."""
        return name in self.tasks

    def finalize(self) -> None:
        """Prevent subsequent task registrations."""
        self._finalized = True


class ResultManager:
    """Manage result backend and store task results."""
    def __init__(self, app: Celery) -> None:
        self.app: Celery = app
        self.backend = None  # type: Optional[Any]

    def get_backend(self) -> Any:
        """Return initialized backend or raise if not set."""
        if self.backend is None:
            raise RuntimeError('Backend not initialized')
        return self.backend

    def init_backend(self) -> Any:
        """Initialize backend applying any configured overrides."""
        if self.backend is None:
            # uses app._get_backend helper.
            self.backend = self.app._get_backend()
        return self.backend

    def store_result(self, task_id: str, result: Any, state: str) -> None:
        """Store task result in backend."""
        if self.backend is None:
            raise RuntimeError('Backend not initialized or disabled')
        return self.backend.store_result(task_id, result, state)

    def get_result(self, task_id: str) -> Any:
        """Get task result by id."""
        if self.backend is None:
            raise RuntimeError('Backend not initialized or disabled')
        return self.backend.get_task_meta(task_id)


class WorkerRunner:
    """Encapsulate worker lifecycle operations."""
    def __init__(self, app: Celery, loader: Any) -> None:
        self.app: Celery = app
        self.loader = loader
        self._initialized = False

    def init_worker(self) -> None:
        """Initialize worker related resources."""
        if not self._initialized:
            self.loader.init_worker()
            self._initialized = True

    def shutdown_worker(self) -> None:
        """Gracefully shutdown worker."""
        self.loader.shutdown_worker()
        self._initialized = False

    def init_worker_process(self) -> None:
        """Initialize resources before task execution."""
        self.loader.init_worker_process()
