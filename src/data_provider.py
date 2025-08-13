from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from database import db_get_map, db_update_from_api, db_upsert_from_scan
from scan_session import ScanSession


class BaseDataProvider(ABC):
    """Interface describing how analyzer accesses beatmap metadata."""

    def __init__(self, session: ScanSession):
        self.session = session

    def _cache_and_return(self, beatmap: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if beatmap:
            self.session.register_beatmap(beatmap)
        return beatmap

    def get_cached(self, identifier: Any, *, by: str = "md5") -> Optional[Dict[str, Any]]:
        return self.session.get_beatmap(identifier, by=by)

    @abstractmethod
    def get_map(self, identifier: Any, *, by: str = "md5") -> Optional[Dict[str, Any]]:
        ...

    @abstractmethod
    def save_scan_result(self, md5: str, data: Dict[str, Any]) -> None:
        ...

    @abstractmethod
    def update_map_from_api(self, beatmap_id: int, data: Dict[str, Any]) -> None:
        ...


class LocalCacheDataProvider(BaseDataProvider):
    """Wrapper around the legacy SQLite cache for custom-keys mode."""

    def get_map(self, identifier: Any, *, by: str = "md5") -> Optional[Dict[str, Any]]:
        beatmap = db_get_map(identifier, by=by)
        return self._cache_and_return(beatmap)

    def save_scan_result(self, md5: str, data: Dict[str, Any]) -> None:
        db_upsert_from_scan(md5, data)
        self._cache_and_return(db_get_map(md5, by="md5"))

    def update_map_from_api(self, beatmap_id: int, data: Dict[str, Any]) -> None:
        db_update_from_api(beatmap_id, data)
        self._cache_and_return(db_get_map(beatmap_id, by="id"))


class ServerDataProvider(BaseDataProvider):
    """Placeholder for the upcoming OAuth/server-backed provider."""

    def __init__(self, session: ScanSession, osu_api_client):
        super().__init__(session)
        self.osu_api_client = osu_api_client

    def get_map(self, identifier: Any, *, by: str = "md5") -> Optional[Dict[str, Any]]:
        cached = self.get_cached(identifier, by=by)
        if cached:
            return cached
        # Actual server-backed lookup will be implemented in the next phase.
        return None

    def save_scan_result(self, md5: str, data: Dict[str, Any]) -> None:
        # OAuth flow stores everything in memory; persistence handled server-side.
        if data:
            snapshot = dict(data)
            snapshot.setdefault("md5_hash", md5)
            self.session.register_beatmap(snapshot)

    def update_map_from_api(self, beatmap_id: int, data: Dict[str, Any]) -> None:
        # Server provider will refresh data from backend soon. For now we just cache.
        snapshot = dict(data)
        snapshot["beatmap_id"] = beatmap_id
        self.session.register_beatmap(snapshot)
