from importlib.metadata import PackageNotFoundError, version

# PyPI distribution name. The import package and CLI are both `dcx`; the
# distribution is published under a different name (see pyproject `[project].name`).
_DIST_NAME = "datacontract-x"

try:
    __version__ = version(_DIST_NAME)
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"
