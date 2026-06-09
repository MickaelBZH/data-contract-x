"""Make CLI-output assertions deterministic regardless of the runner's color env.

CI (e.g. GitHub Actions) sets ``FORCE_COLOR``, which makes Typer/rich render help
and error text as ANSI panels — the styling/wrapping breaks plain substring checks
on ``result.output`` (locally, where output is a non-TTY, it renders plain). Force
plain, wide output so ``CliRunner`` results are stable on every machine.
"""

import os

os.environ.pop("FORCE_COLOR", None)
os.environ["NO_COLOR"] = "1"
os.environ["COLUMNS"] = "200"
