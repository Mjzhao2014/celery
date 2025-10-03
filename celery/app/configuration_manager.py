# -*- coding: utf-8 -*-
"""Configuration management adhering to SRP."""
from __future__ import annotations

import ast
import json
import os
import re
from collections.abc import Mapping
from types import ModuleType
from typing import Any, Dict

from celery.exceptions import ImproperlyConfigured


_CAST_PATTERN = re.compile(r'^\((\w+)\)')


class ConfigurationManager:
    """Encapsulate configuration-related behaviour for a Celery app."""

    def __init__(self, app: "Celery") -> None:
        self.app = app
        self.loader = self.app.loader
        self.override_backends: Dict[str, str] = dict(
            getattr(self.loader, 'override_backends', {}) or {})
        self.conf = self.app.conf

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _sync_override_backends(self) -> None:
        loader_overrides = getattr(self.loader, 'override_backends', None)
        if loader_overrides is not None:
            self.override_backends = dict(loader_overrides)
        else:
            overrides = self.app.conf.get('override_backends', {}) or {}
            self.override_backends = dict(overrides)

    def _update_conf(self, data: Dict[str, Any]) -> None:
        if data:
            self.app.conf.update(data)
        self._sync_override_backends()
        self.conf = self.app.conf

    def _mapping_from_object(self, obj: Any) -> Dict[str, Any]:
        if isinstance(obj, Mapping):
            return dict(obj)
        if isinstance(obj, ModuleType):
            return {
                key: getattr(obj, key)
                for key in dir(obj)
                if not key.startswith('_')
            }
        if hasattr(obj, 'items') and callable(obj.items):
            return dict(obj.items())
        raise ImportError(f"Invalid configuration object: {obj!r}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def config_from_object(self, obj, silent: bool = False):
        try:
            if isinstance(obj, str):
                try:
                    if not self.loader.config_from_object(obj, silent=False):
                        raise ImportError(f"Unable to import configuration {obj!r}")
                except (ImportError, AttributeError, ValueError) as exc:
                    raise ImportError(str(exc)) from exc
                self._update_conf({})
            else:
                mapping = self._mapping_from_object(obj)
                self._update_conf(mapping)
        except ImportError:
            if silent:
                return None
            raise
        try:
            self.app.result_manager.init_backend()
        except Exception:  # pragma: no cover - backend may be optional
            pass
        return self.conf

    def read_configuration(self, env: str = 'CELERY_CONFIG_MODULE'):
        module_name = os.environ.get(env)
        if not module_name:
            raise ImproperlyConfigured(
                f"The environment variable {env!r} is not set")
        return self.config_from_object(module_name, silent=False)

    def _cast_value(self, raw: str):
        casters = {
            'int': int,
            'float': float,
            'bool': lambda v: v.strip().lower() in {'1', 'true', 'yes', 'on'},
            'str': str,
            'list': ast.literal_eval,
            'tuple': ast.literal_eval,
            'dict': ast.literal_eval,
            'set': ast.literal_eval,
            'json': json.loads,
            'regex': re.compile,
        }
        match = _CAST_PATTERN.match(raw)
        if match:
            type_name = match.group(1).lower()
            caster = casters.get(type_name)
            if caster is None:
                raise ValueError(f"Unsupported cast type {type_name!r}")
            remainder = raw[match.end():]
            return caster(remainder)
        lowered = raw.lower()
        if lowered in {'true', 'false'}:
            return lowered == 'true'
        for caster in (int, float):
            try:
                return caster(raw)
            except (TypeError, ValueError):
                continue
        if raw and raw[0] in "[{(" and raw[-1] in "]})":
            try:
                return ast.literal_eval(raw)
            except (ValueError, SyntaxError):
                pass
        return raw

    def cmdline_config_parser(self, args, **kwargs) -> dict:
        parsed = {}
        for arg in args:
            key, value = arg.split('=', 1)
            parsed[key] = self._cast_value(value)
        return parsed
