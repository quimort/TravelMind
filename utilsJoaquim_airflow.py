"""Compatibility shim so code that does `import utilsJoaquim` still works.

This re-exports the implementation from the `landing` package where the
original module lives (`landing/utilsJoaquim.py`). Placing a small shim at
project root makes the module importable as a top-level name inside the
Airflow containers (since the repository root is on PYTHONPATH).
"""
from landing.utilsJoaquim_airflow import *  # noqa: F401,F403

__all__ = []
