"""Process entry the controller spawns.

``python -m mftik_sts.session_worker <request.json>`` reads one
``StsCreateSessionRequest`` and walks phases 0–6.
"""

from __future__ import annotations

from mftik_sts.session_worker.process import main as run


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
