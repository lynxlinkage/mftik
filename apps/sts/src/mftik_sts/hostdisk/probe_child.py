"""Subprocess entry: import one tree and print a one-line report.

Run as a script (``python probe_child.py``), not as
``python -m mftik_sts.hostdisk.probe_child``. The ``-m`` form loads
``mftik_sts``, and that package imports the running STS process. This
file imports :func:`mftik.registry.load.load_class` and
:func:`mftik.registry.protocol.handshake_info`. The controller does not.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        _emit("skipped", "import error: probe arguments were not a tree")
        return 0
    tree, data_dir, digest, generation = argv
    try:
        from mftik.environment import NodeEnv
        from mftik.registry.files import read_tree
        from mftik.registry.gate import check_files
        from mftik.registry.inspect import pick_class
        from mftik.registry.load import load_class
        from mftik.registry.protocol import handshake_info

        info = handshake_info(data_dir=data_dir)
        if not isinstance(info, dict) or "mftik_version" not in info:
            _emit("skipped", "import error: handshake was not registry info")
            return 0
        if generation != "-":
            site = NodeEnv(data_dir).site_packages(int(generation))
            if site.is_dir():
                sys.path.insert(0, str(site))
        files = read_tree(Path(tree))
        chosen = pick_class(check_files(files))
        load_class(
            Path(tree),
            type_name=chosen.type,
            source="probe",
            name=chosen.type,
            digest=digest,
        )
    except Exception as exc:
        _emit("skipped", f"import error: {exc}")
        return 0
    _emit("loaded", None)
    return 0


def _emit(status: str, reason: str | None) -> None:
    json.dump({"status": status, "reason": reason}, sys.stdout)
    sys.stdout.write("\n")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
