import nox


PYTHON_VERSIONS = ["3.7", "3.8", "3.9", "3.10", "3.11"]


def _setup_session_requirements(session, extra_packages=[]):
    session.install("--upgrade", "pip", "wheel")
    session.install("-e", ".[dev]")

    if extra_packages:
        session.install(*extra_packages)


@nox.session(python=PYTHON_VERSIONS, venv_backend="venv")
def unit(session):
    _setup_session_requirements(session)
    session.run("pytest", "tests/unit")
