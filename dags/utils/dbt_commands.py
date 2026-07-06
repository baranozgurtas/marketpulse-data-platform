"""
MarketPulse - dbt command builder

Pure functions that build the shell commands executed by the dbt
Airflow DAG's BashOperators. Kept free of Airflow imports so the
logic is unit-testable (see tests/test_dbt_commands.py).
"""

DEFAULT_DBT_PROJECT_DIR = "/opt/airflow/dbt"

# Commands the DAG is allowed to run. Anything else is a mistake.
ALLOWED_DBT_COMMANDS = ("run", "test", "debug", "compile", "parse")


def build_dbt_command(command, project_dir=None, select=None):
    """
    Build a dbt CLI invocation string.

    Args:
        command: dbt subcommand, one of ALLOWED_DBT_COMMANDS.
        project_dir: dbt project directory; also used as profiles dir
            since profiles.yml lives inside the project (env-var driven,
            no credentials on disk).
        select: optional dbt --select expression.

    Returns:
        The full shell command string.

    Raises:
        ValueError: if command is not an allowed dbt subcommand.
    """
    if command not in ALLOWED_DBT_COMMANDS:
        raise ValueError(
            f"Unsupported dbt command '{command}'. "
            f"Allowed: {', '.join(ALLOWED_DBT_COMMANDS)}"
        )

    project_dir = project_dir or DEFAULT_DBT_PROJECT_DIR

    parts = [
        "dbt",
        command,
        f"--project-dir {project_dir}",
        f"--profiles-dir {project_dir}",
    ]
    if select:
        parts.append(f"--select {select}")

    return " ".join(parts)
