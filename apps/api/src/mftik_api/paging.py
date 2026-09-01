"""How far a list may be paged, and the parameter that says so.

Here rather than in ``deps.py`` because both halves of the answer need it:
the routes, to bound the ``offset`` they accept, and the schemas, to tell
a client the bound rather than making it keep its own copy.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Query

#: How far a numbered browse may page.
#:
#: An offset page makes Postgres walk every index entry it skips, and
#: nothing else bounds that: ``limit`` caps a page at 500 rows, but
#: ``offset=100_000_000`` is one cheap request that costs the database a
#: hundred million entries to answer with nothing. This is the far side of
#: any real browse — page 2,001 at 50 rows a page — so past it the answer
#: is a 422, not a slow scan.
MAX_LIST_OFFSET = 100_000

#: The ``offset`` of every list that pages, so they cannot drift apart.
ListOffset = Annotated[int, Query(ge=0, le=MAX_LIST_OFFSET)]
