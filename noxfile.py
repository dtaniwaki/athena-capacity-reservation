"""Nox sessions for athena-capacity-reservation."""

import nox

PYTHON_VERSIONS = ["3.11", "3.12", "3.13"]

nox.options.default_venv_backend = "uv"


@nox.session(python=PYTHON_VERSIONS)
def test(session: nox.Session) -> None:
    session.install("-e", ".[dev,slack]")
    session.run(
        "pytest",
        "--cov=athena_capacity_reservation",
        "--cov-report=term-missing",
        "--cov-report=markdown:coverage.md",
    )


@nox.session(python=PYTHON_VERSIONS[-1])
def lint(session: nox.Session) -> None:
    session.install("-e", ".[dev]")
    session.run("ruff", "check", "src/", "tests/")


@nox.session(python=PYTHON_VERSIONS[-1])
def typecheck(session: nox.Session) -> None:
    session.install("-e", ".[dev,slack]")
    session.run("mypy")


@nox.session(python=PYTHON_VERSIONS[-1])
def security(session: nox.Session) -> None:
    session.install("-e", ".[dev]")
    session.run("bandit", "-r", "src/", "-ll")
    session.run("pip-audit")
