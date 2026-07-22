"""Render multi-line strings as YAML block scalars.

Upstream's `OpenDataContractStandard.to_yaml()` calls `yaml.dump` with the default
`yaml.Dumper`, which emits multi-line strings as double-quoted flow scalars full of
`\\n` escapes and line-continuation backslashes. That is unreadable for the values
that are *meant* to be read as text — most of all a view's `viewDefinition` SQL, but
also long descriptions — and it makes contract diffs one-giant-line changes.

Registering a `str` representer on `yaml.Dumper` switches those to `|` block scalars.
PyYAML falls back to a quoted style on its own when a block scalar cannot round-trip
the value (e.g. a line with trailing whitespace), so this is lossless. Single-line
strings are unaffected, as is `yaml.safe_dump` (a different Dumper class), which the
dbt exporter uses.

Imported for side effects by `dcx.cli` and `dcx.api`.
"""

import yaml


def _represent_str(dumper: yaml.Dumper, data: str) -> yaml.ScalarNode:
    style = "|" if "\n" in data else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=style)


yaml.add_representer(str, _represent_str)
