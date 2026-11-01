"""Import the installed mftik wheel and check the package version."""

import importlib.metadata
import sys
from pathlib import Path

import mftik

# scripts/release-check versions keeps this equal to the workspace version.
EXPECTED_VERSION = "1.3.0"


def main() -> None:
    module_file = getattr(mftik, "__file__", None)
    if not isinstance(module_file, str):
        sys.exit("mftik imported without a file path")
    module_path = Path(module_file).resolve()
    repo_root = Path(__file__).resolve().parents[2]
    if module_path == repo_root or repo_root in module_path.parents:
        sys.exit(f"imported mftik from the source tree ({module_path})")

    module_version = getattr(mftik, "__version__", None)
    if module_version is not None and module_version != EXPECTED_VERSION:
        sys.exit(f"mftik.__version__ is {module_version!r}, expected {EXPECTED_VERSION!r}")

    try:
        distribution_version = importlib.metadata.version("mftik")
    except importlib.metadata.PackageNotFoundError:
        distribution_version = None
    if distribution_version != EXPECTED_VERSION:
        sys.exit(
            f"installed mftik version is {distribution_version!r}, expected {EXPECTED_VERSION!r}"
        )

    shown = module_version if module_version is not None else distribution_version
    print(f"imported mftik {shown} from {module_path}")


if __name__ == "__main__":
    main()
