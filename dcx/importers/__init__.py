"""Live importers — `dcx import <system>` (`snowflake`, `kafka`, ...).

One file per source system. `registry.py` registers each importer with the
upstream `importer_factory` and adds the matching `dcx import` subcommand; it is
imported for its side effects by `dcx.cli`. To add a source, drop a new module
here and register it in `registry.py`.
"""
