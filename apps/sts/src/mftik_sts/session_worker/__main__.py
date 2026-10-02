"""Process entry the controller spawns. B4-03 implements the worker.

The controller runs ``python -m mftik_sts.session_worker <request.json>``.
The file is UTF-8 JSON of the ``StsCreateSessionRequest`` the API sent.
This entry refuses to run so a spawn before that ticket is a visible
failure rather than a process that looks idle.
"""

from __future__ import annotations

import sys


def main() -> None:
    sys.stderr.write("mftik_sts.session_worker is not implemented (B4-03)\n")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
