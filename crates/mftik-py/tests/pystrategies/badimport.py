"""Importing this module fails after leaving a partial stdout line."""

import sys

sys.stdout.write("TAIL-import")
raise ImportError("missing piece")
