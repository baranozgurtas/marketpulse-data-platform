"""
MarketPulse - Unit Tests for dbt Command Builder
Tests the pure command-construction logic used by the dbt DAG,
without requiring Airflow or dbt to be installed.
Run: pytest tests/test_dbt_commands.py
"""

import os
import sys

import pytest

# The helper lives inside the Airflow dags folder (Airflow puts that
# folder on sys.path at runtime; we do the same for tests).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from utils.dbt_commands import (  # noqa: E402
    ALLOWED_DBT_COMMANDS,
    DEFAULT_DBT_PROJECT_DIR,
    build_dbt_command,
)


class TestBuildDbtCommand:
    def test_run_command(self):
        cmd = build_dbt_command("run", project_dir="/opt/airflow/dbt")
        assert cmd == (
            "dbt run --project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/dbt"
        )

    def test_test_command(self):
        cmd = build_dbt_command("test", project_dir="/opt/airflow/dbt")
        assert cmd.startswith("dbt test ")

    def test_default_project_dir(self):
        cmd = build_dbt_command("run")
        assert f"--project-dir {DEFAULT_DBT_PROJECT_DIR}" in cmd
        assert f"--profiles-dir {DEFAULT_DBT_PROJECT_DIR}" in cmd

    def test_select_flag(self):
        cmd = build_dbt_command("run", select="marts")
        assert cmd.endswith("--select marts")

    def test_no_select_flag_when_omitted(self):
        cmd = build_dbt_command("run")
        assert "--select" not in cmd

    def test_rejects_unknown_command(self):
        with pytest.raises(ValueError):
            build_dbt_command("seed")

    def test_rejects_injection_like_command(self):
        """Only whitelisted subcommands are accepted."""
        with pytest.raises(ValueError):
            build_dbt_command("run; rm -rf /")

    def test_all_allowed_commands_build(self):
        for command in ALLOWED_DBT_COMMANDS:
            cmd = build_dbt_command(command)
            assert cmd.startswith(f"dbt {command} ")
