"""Strategy-side OMS mirror — fed from ``td.oms.{api_id}``."""

from __future__ import annotations

from mft.exchange.oms import OmsView


class StrategyOms:
    """Read API for reconciled / live OMS state, keyed by TD ``api_id``."""

    def __init__(self) -> None:
        self._views: dict[int, OmsView] = {}

    def update(self, api_id: int, view: OmsView) -> None:
        self._views[api_id] = view

    def get(self, api_id: int) -> OmsView | None:
        return self._views.get(api_id)

    def __getitem__(self, api_id: int) -> OmsView:
        return self._views[api_id]

    def __contains__(self, api_id: object) -> bool:
        return isinstance(api_id, int) and api_id in self._views

    @property
    def api_ids(self) -> list[int]:
        return list(self._views)

    def view(self, api_id: int | None = None) -> OmsView | None:
        """Return OMS for ``api_id``, or the sole account if only one is present."""
        if api_id is not None:
            return self._views.get(api_id)
        if len(self._views) == 1:
            return next(iter(self._views.values()))
        return None
