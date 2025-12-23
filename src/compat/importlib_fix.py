# src/compat/importlib_fix.py
import sys

if sys.version_info < (3, 10):
    import importlib_metadata
    import importlib

    if not hasattr(importlib.metadata, "packages_distributions"):
        importlib.metadata.packages_distributions = (
            importlib_metadata.packages_distributions
        )