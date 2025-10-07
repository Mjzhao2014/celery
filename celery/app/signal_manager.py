# -*- coding: utf-8 -*-
"""Signal management adhering to SRP."""
from __future__ import annotations

import inspect
from typing import Callable, Dict, Optional, Tuple

from celery import signals
from celery.utils.dispatch import Signal


class SignalManager:
    """Manage built-in and custom signals for a Celery application."""

    def __init__(self, app: "Celery") -> None:
        self.app = app
        self._custom_signals: Dict[str, Signal] = {}
        self._builtin_signals: Dict[str, Signal] = {}
        self._handler_wrappers: Dict[Tuple[Signal, Callable], Callable] = {}
        for name in dir(signals):
            sig = getattr(signals, name)
            if isinstance(sig, Signal):
                self._builtin_signals[name] = sig
        # back-compat name expected in tests
        if 'celeryd_init' in self._builtin_signals:
            self._builtin_signals.setdefault(
                'celeryd_startup', self._builtin_signals['celeryd_init'])

    def _resolve_signal(self, sig_or_name) -> Signal:
        if isinstance(sig_or_name, Signal):
            return sig_or_name
        name = sig_or_name
        if name in self._builtin_signals:
            return self._builtin_signals[name]
        if name in self._custom_signals:
            return self._custom_signals[name]
        raise KeyError(name)

    def _introspect_handler(self, handler: Callable) -> tuple[bool, Optional[set[str]]]:
        try:
            signature = inspect.signature(handler)
        except (TypeError, ValueError):
            return True, None

        accepts_kwargs = any(
            param.kind == param.VAR_KEYWORD
            for param in signature.parameters.values()
        )
        accepted_keywords = {
            name
            for name, param in signature.parameters.items()
            if param.kind in (param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY)
        }
        return accepts_kwargs, accepted_keywords

    def connect(self, signal: Signal | str, handler: Callable, **kwargs):
        sig = self._resolve_signal(signal)
        accepts_kwargs, accepted = self._introspect_handler(handler)

        def can_accept(name: str) -> bool:
            if accepts_kwargs or accepted is None:
                return True
            return name in accepted

        wants_sender = can_accept('sender')
        wants_signal = can_accept('signal')

        def wrapper(sender=None, signal=None, **inner):
            call_kwargs: Dict[str, object] = {}
            if wants_sender:
                call_kwargs['sender'] = sender
            if wants_signal:
                call_kwargs['signal'] = signal
            if accepts_kwargs:
                call_kwargs.update(inner)
            elif accepted:
                for key, value in inner.items():
                    if key in accepted:
                        call_kwargs[key] = value
            return handler(**call_kwargs)

        self._handler_wrappers[(sig, handler)] = wrapper
        return sig.connect(wrapper, **kwargs)

    def disconnect(self, signal: Signal | str, handler: Callable):
        sig = self._resolve_signal(signal)
        wrapper = self._handler_wrappers.pop((sig, handler), None)
        if wrapper is None:
            wrapper = handler
        return sig.disconnect(wrapper)

    def emit(self, signal: Signal | str, **kwargs):
        sig = self._resolve_signal(signal)
        return sig.send(sender=self.app, **kwargs)

    def register_signal(self, name: str) -> Signal:
        if name in self._builtin_signals or name in self._custom_signals:
            raise ValueError(f"Signal {name!r} already exists")
        sig = Signal(name=name)
        self._custom_signals[name] = sig
        return sig

    def remove_signal(self, signal: Signal | str) -> None:
        name = signal if isinstance(signal, str) else signal.name
        if name in self._builtin_signals:
            raise KeyError(f"Cannot remove built-in signal {name!r}")
        try:
            del self._custom_signals[name]
        except KeyError as exc:
            raise KeyError(name) from exc

    def get_signal(self, name: str) -> Signal:
        return self._resolve_signal(name)

    def has_signal(self, name: str) -> bool:
        return name in self._builtin_signals or name in self._custom_signals

    def _register_builtin_signal(self, name: str, sig: Signal) -> None:
        self._builtin_signals[name] = sig
