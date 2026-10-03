"""MFTIK strategy domain — the strategy registry, its environment and runtime.

The controller process is :func:`mftik_sts.app.main` (``python -m mftik_sts``
and the ``sts`` console script). This package does not import
:mod:`mftik_sts.app`. The session worker's entry is
``python -m mftik_sts.session_worker``, and importing this package on
the way there must not load :mod:`mftik_db` (F10, B5-09). Session status
is written by the controller's Supervisor; the worker does not open a
database.
"""
