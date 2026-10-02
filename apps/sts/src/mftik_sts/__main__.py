"""STS controller process.

Import :mod:`mftik_sts.app` here. The package ``__init__`` does not,
so ``python -m mftik_sts.session_worker`` does not load the controller
or the database (F10, B5-09).
"""

from mftik_sts.app import main

if __name__ == "__main__":
    main()
