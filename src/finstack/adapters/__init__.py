"""Source adapters: one pipeline per data type. Importing this package registers them all with the router."""
from . import crypto, global_, india_company, india_derivs, india_prices, macro, news, reference  # noqa: F401
