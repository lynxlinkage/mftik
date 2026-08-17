"""Exit codes, in one place because more than one module has to mean them.

They are a contract: a CI job retries a node that did not answer and does not
retry a document that will not parse. ``run`` needs them too, and importing
them from the parser module that dispatches to ``run`` would be a cycle.
"""

from __future__ import annotations

#: Something the user can fix: a bad argument, a refused request, a 404.
EXIT_ERROR = 1
#: The node did not answer. Separated so a script can retry this and not that.
EXIT_UNREACHABLE = 2
#: Interrupted. The shell convention, so ``mftik run`` stopped with Ctrl-C
#: reports the same thing every other program does.
EXIT_INTERRUPTED = 130
