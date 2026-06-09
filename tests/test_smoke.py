from typer.testing import CliRunner

from dcx.cli import app

runner = CliRunner()


def test_help_includes_inherited_commands():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for cmd in ("init", "lint", "test", "export", "import", "info"):
        assert cmd in result.stdout


def test_info_prints_both_versions():
    result = runner.invoke(app, ["info"])
    assert result.exit_code == 0
    assert "dcx" in result.stdout
    assert "datacontract-cli" in result.stdout
