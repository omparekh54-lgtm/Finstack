"""Source adapters: one pipeline per data type. Importing this package registers them all with the router.

Each adapter module is loaded on its own: if one fails to import (a library changed, a typo in a new
module), finstack prints one warning and keeps every other data type working.
"""
import importlib
import sys

STABLE = ("crypto", "global_", "india_company", "india_derivs", "india_prices", "macro", "news", "reference")
BETA = ("india_extra", "global_extra")

failed = {}
for _name in STABLE + BETA:
    try:
        importlib.import_module(f"{__name__}.{_name}")
    except Exception as _e:  # noqa: BLE001 - one broken module must not take finstack down
        failed[_name] = f"{type(_e).__name__}: {_e}"
        print(f"[finstack] warning: data types in '{_name}' are unavailable ({failed[_name][:200]})",
              file=sys.stderr)
