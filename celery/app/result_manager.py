# -*- coding: utf-8 -*-
"""Result management adhering to SRP."""
from __future__ import annotations

import threading
from types import SimpleNamespace
from typing import Any, Optional, Tuple, Type

from celery.app import backends
from celery.backends.base import DisabledBackend
import base64
import pickle

from celery.exceptions import ImproperlyConfigured


_RAW_EXCEPTION_MARKER = '__celery_result_manager_raw__'
_MISSING = object()


class ResultManager:
    """Coordinate result backend lifecycle and interactions."""

    def __init__(self, app: "Celery") -> None:
        self.app = app
        self._backend_cls: Optional[Type] = None
        self._backend_url: Optional[str] = None
        self._backend_instance = None
        self._failure_cache: dict[str, Any] = {}
        self._backend_local = threading.local()
        existing_backend = getattr(app, '_backend', None)
        if existing_backend is not None:
            self.backend = existing_backend

    @property
    def backend(self):
        return getattr(self._backend_local, 'instance', None)

    @backend.setter
    def backend(self, value):
        if value is None:
            if hasattr(self._backend_local, 'instance'):
                del self._backend_local.instance
        else:
            self._backend_local.instance = value

    def _encode_raw_result(self, value: Any) -> str:
        payload = pickle.dumps(value)
        return base64.b64encode(payload).decode('ascii')

    def _decode_raw_result(self, payload: Any) -> Any:
        if isinstance(payload, bytes):
            payload = payload.decode('ascii')
        data = base64.b64decode(payload)
        return pickle.loads(data)

    def _wrap_failure_payload(self, value: Any) -> dict[str, Any]:
        encoded = self._encode_raw_result(value)
        if isinstance(value, BaseException):
            type_name = type(value).__qualname__
            module_name = type(value).__module__
        else:
            type_name = 'Exception'
            module_name = 'builtins'
        return {
            'exc_type': type_name,
            'exc_module': module_name,
            'exc_message': (_RAW_EXCEPTION_MARKER, encoded),
        }

    def _unwrap_failure_payload(self, payload: Any) -> Any:
        if isinstance(payload, dict):
            message = payload.get('exc_message')
            if isinstance(message, (tuple, list)) and message and message[0] == _RAW_EXCEPTION_MARKER:
                try:
                    return self._decode_raw_result(message[1])
                except Exception:  # pragma: no cover - corrupted payloads fall back
                    return message[1]
        if isinstance(payload, BaseException) and payload.args and payload.args[0] == _RAW_EXCEPTION_MARKER:
            try:
                return self._decode_raw_result(payload.args[1])
            except Exception:  # pragma: no cover - corrupted payloads fall back
                return payload.args[1]
        return payload

    def _resolve_backend_setting(self):
        return self.app.backend_cls or self.app.conf.result_backend

    def _apply_overrides(self, setting):
        overrides = getattr(self.app.config_manager, 'override_backends', {}) or {}
        if isinstance(setting, str):
            setting = overrides.get(setting, setting)
            if setting == 'memory://':
                setting = 'cache+memory://'
        return setting

    def _prepare_backend_definition(self) -> Tuple[Optional[Type], Optional[str]]:
        if self._backend_cls is not None or self._backend_instance is not None:
            return self._backend_cls, self._backend_url

        setting = self._apply_overrides(self._resolve_backend_setting())

        try:
            if isinstance(setting, str) or setting is None:
                backend_cls, url = backends.by_url(setting, self.app.loader)
                self._backend_cls, self._backend_url = backend_cls, url
            elif isinstance(setting, type):
                self._backend_cls, self._backend_url = setting, None
            else:
                setting.app = self.app
                self._backend_instance = setting
        except ImproperlyConfigured as exc:
            raise ImportError(str(exc)) from exc

        return self._backend_cls, self._backend_url

    def init_backend(self):
        backend = self.backend
        if backend is not None:
            return backend

        backend_cls, backend_url = self._prepare_backend_definition()
        if self._backend_instance is not None and backend_cls is None:
            backend = self._backend_instance
        else:
            backend = backend_cls(app=self.app, url=backend_url)

        self.backend = backend
        self.app._backend = backend
        return backend

    def get_backend(self):
        backend = self.backend
        if backend is None:
            # if self._backend_cls is None and self._backend_instance is None:
            raise RuntimeError('Result backend not initialized')
            # backend = self.init_backend()
        return backend

    def _ensure_backend_ready(self):
        backend = self.get_backend()
        if isinstance(backend, DisabledBackend):
            raise RuntimeError('Result backend is disabled')
        return backend

    def store_result(self, task_id, result, state):
        backend = self._ensure_backend_ready()
        if state in getattr(backend, 'EXCEPTION_STATES', ()):  # pragma: no branch - attribute exists on all builtin backends
            self._failure_cache[task_id] = result
            result = self._wrap_failure_payload(result)
        try:
            return backend.store_result(task_id, result, state)
        except NotImplementedError as exc:
            raise RuntimeError(str(exc)) from exc

    def _normalize_result(self, meta: dict[str, Any], backend) -> SimpleNamespace:
        meta = meta or {}
        status = meta.get('status', meta.get('state'))
        result = meta.get('result', meta.get('retval'))
        task_id = meta.get('task_id', meta.get('id'))
        if status in getattr(backend, 'EXCEPTION_STATES', ()):  # pragma: no branch
            cached = self._failure_cache.get(task_id, _MISSING)
            if cached is not _MISSING:
                result = cached
            else:
                result = self._unwrap_failure_payload(result)
        return SimpleNamespace(
            task_id=task_id,
            result=result,
            status=status,
            traceback=meta.get('traceback'),
            children=meta.get('children'),
        )

    def get_result(self, task_id):
        backend = self._ensure_backend_ready()
        meta = backend.get_task_meta(task_id)
        return self._normalize_result(meta, backend)
