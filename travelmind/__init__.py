"""Compatibility package `travelmind`.

This package is a tiny shim so that imports like

    from travelmind.landing import ...

will resolve to the existing top-level `landing/` directory in the project
when the project root is on sys.path (for example via /opt/airflow/include/*.pth
or PYTHONPATH). It does this by adding the project root to the package
`__path__` so subpackages found at the repo root (like `landing`) are
discoverable as `travelmind.<subpkg>`.
"""
import os

# Insert the project root (parent of this travelmind/ package) at the front
# of the package search path so subpackages like `landing` (which live at
# <project_root>/landing) are importable as travelmind.landing.
_here = os.path.abspath(os.path.dirname(__file__))
_project_root = os.path.abspath(os.path.join(_here, ".."))
if _project_root not in __path__:
    __path__.insert(0, _project_root)

__all__ = []
