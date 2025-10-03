# -*- coding: utf-8 -*-
"""Module containing the SignalManager abstraction.

The SignalManager is responsible for orchestrating all signal-related
behaviour for a Celery application. It exposes an API for connecting
and disconnecting handlers to signals, for emitting signals and for
creating/destroying user-defined custom signals. The manager is also
aware of built-in Celery signals (both the global signals in
``celery.signals`` and any built-in instance-level signals) so those
may not be removed by users.
"""
from celery.utils.dispatch import Signal
from celery import signals


class SignalManager:
    """Manage built-in and custom signals for a Celery app."""
    def __init__(self, app: "Celery") -> None:
        # circular import safe due to typing quote
        self.app = app
        self._custom_signals = {}
        # Populate built-in mapping from global celery.signals
        self._builtin_signals = {}
        for name in dir(signals):
            sig = getattr(signals, name)
            if isinstance(sig, Signal):
                self._builtin_signals[name] = sig

    def _resolve_signal(self, sig_or_name) -> Signal:
        """Resolve a signal or name to a Signal instance."""
        if isinstance(sig_or_name, Signal):
            return sig_or_name
        name = sig_or_name
        if name in self._builtin_signals:
            return self._builtin_signals[name]
        if name in self._custom_signals:
            return self._custom_signals[name]
        raise KeyError(name)

    def connect(self, signal: Signal, handler, **kwargs):
        """Connect a handler to a signal."""
        sig = self._resolve_signal(signal)
        return sig.connect(handler, **kwargs)

    def disconnect(self, signal, handler):
        """Disconnect a handler from a signal."""
        sig = self._resolve_signal(signal)
        return sig.disconnect(handler)

    def emit(self, signal: Signal, **kwargs):
        """Emit a signal to any connected handlers."""
        sig = self._resolve_signal(signal)
        return sig.send(sender=self.app, **kwargs)

    def register_signal(self, name: str) -> Signal:
        """Register a new custom signal by name."""
        if name in self._builtin_signals or name in self._custom_signals:
            raise ValueError(f"Signal {name!r} already exists")
        sig = Signal(name=name)
        self._custom_signals[name] = sig
        return sig

    def remove_signal(self, signal):
        """Remove a custom signal by name or instance."""
        name = signal if isinstance(signal, str) else signal.name
        if name in self._builtin_signals:
            raise KeyError(f"Cannot remove built-in signal {name!r}")
        try:
            del self._custom_signals[name]
        except KeyError:
            raise KeyError(name)

    def get_signal(self, name: str) -> Signal:
        """Get a signal by name."""
        return self._resolve_signal(name)

    def has_signal(self, name: str) -> bool:
        """Check if a signal exists by name."""
        return name in self._builtin_signals or name in self._custom_signals

    def _register_builtin_signal(self, name: str, sig: Signal) -> None:
        """Register a built-in instance-level signal so it cannot be removed."""
        self._builtin_signals[name] = sig
