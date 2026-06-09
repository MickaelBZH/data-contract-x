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
