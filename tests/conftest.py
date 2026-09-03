"""Stable CLI-output assertions across machines.

CI (e.g. GitHub Actions) sets ``FORCE_COLOR``, so Typer/rich renders help and
error text as ANSI panels — the color codes break plain substring checks on
``result.output``, and a narrow terminal would wrap option names mid-token. We
pin a wide width (no wrapping) and expose a ``strip_ansi`` fixture so tests can
assert against plain text regardless of the runner's color settings.
"""

import os
import re

import pytest

# Fixed wide width so rich never wraps an option name across lines.
os.environ["COLUMNS"] = "200"

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


@pytest.fixture
def strip_ansi():
    """Return a function that strips ANSI color codes from CLI output."""
    return lambda text: _ANSI_RE.sub("", text)


@pytest.fixture
def set_connection_profiles(monkeypatch):
    """Return a helper that replaces the Connector's named profiles."""
    import snowflake.connector.config_manager as config_manager

    class FakeConfigManager:
        def __init__(self, profiles):
            self.profiles = profiles

        def __getitem__(self, key):
            if key == "connections":
                return self.profiles
            raise KeyError(key)

    def set_profiles(profiles):
        monkeypatch.setattr(config_manager, "CONFIG_MANAGER", FakeConfigManager(profiles))

    return set_profiles
