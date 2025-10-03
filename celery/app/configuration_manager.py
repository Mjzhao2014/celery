# -*- coding: utf-8 -*-
"""ConfigurationManager encapsulates all configuration related logic.

It deals with loading configuration from objects, modules or the
environment, exposes the loader used by the application to parse
configuration and keeps track of any override_backends specified via
configuration.
"""
import os

from celery.exceptions import ImproperlyConfigured


class ConfigurationManager:
    """Manage configuration lifecycle for the Celery application."""
    def __init__(self, app: "Celery") -> None:
        # circular import avoided via type string
        self.app = app
        # bind to the loader used to import configuration modules
        self.loader = self.app.loader
        # capture any existing override_backends the loader may have
        self.override_backends: dict = getattr(self.loader, 'override_backends', {})
        # the final configuration dict, may be updated once config loaded
        self.conf = None

    def config_from_object(self, obj, silent: bool = False):
        """Apply configuration from an object or module."""
        if self.loader.config_from_object(obj, silent=silent):
            # capture any override_backends from loader
            if getattr(self.loader, 'override_backends', None) is not None:
                self.override_backends = self.loader.override_backends
            self.conf = self.app.conf
            return self.conf

    def read_configuration(self, env: str = 'CELERY_CONFIG_MODULE'):
        """Read configuration from an environment variable and return dict."""
        module_name = os.environ.get(env)
        if not module_name:
            raise ImproperlyConfigured(
                f"The environment variable {env!r} is not set")
        return self.config_from_object(module_name, silent=False)

    def cmdline_config_parser(self, args, **kwargs) -> dict:
        """Parse command-line arguments and return configuration dict."""
        return self.loader.cmdline_config_parser(args, **kwargs)
