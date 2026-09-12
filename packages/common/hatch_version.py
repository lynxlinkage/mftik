"""Distribution version for hatchling. Not imported at runtime."""

from __future__ import annotations

import os

__version__ = os.environ.get("MFTIK_DIST_VERSION", "0.0.0")
