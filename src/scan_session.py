from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class ScanSession:
    """In-memory container for data captured during a scan."""

    user_id: Optional[int] = None
    username: str = ""
    started_at: datetime = field(default_factory=datetime.utcnow)

    metadata: Dict[str, Any] = field(default_factory=dict)
    summary_stats: Dict[str, Any] = field(default_factory=dict)

    top_scores: List[Dict[str, Any]] = field(default_factory=list)
    lost_scores: List[Dict[str, Any]] = field(default_factory=list)
    replay_manifest: List[Dict[str, Any]] = field(default_factory=list)

    beatmaps_by_md5: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    beatmaps_by_id: Dict[int, Dict[str, Any]] = field(default_factory=dict)

    def clear(self) -> None:
        """Reset session data while keeping the object instance."""
        self.metadata.clear()
        self.summary_stats.clear()
        self.top_scores.clear()
        self.lost_scores.clear()
        self.replay_manifest.clear()
        self.beatmaps_by_md5.clear()
        self.beatmaps_by_id.clear()
        self.started_at = datetime.utcnow()

    def register_beatmap(self, beatmap_data: Dict[str, Any]) -> None:
        if not beatmap_data:
            return

        snapshot = dict(beatmap_data)

        md5 = snapshot.get("md5_hash")
        if md5:
            self.beatmaps_by_md5[str(md5)] = snapshot

        beatmap_id = snapshot.get("beatmap_id")
        if beatmap_id is not None:
            try:
                key = int(beatmap_id)
            except (ValueError, TypeError):
                return
            self.beatmaps_by_id[key] = snapshot

    def get_beatmap(self, identifier: Any, *, by: str = "md5") -> Optional[Dict[str, Any]]:
        if identifier is None:
            return None

        if by == "id":
            try:
                key = int(identifier)
            except (ValueError, TypeError):
                return None
            return self.beatmaps_by_id.get(key)

        if by == "md5":
            return self.beatmaps_by_md5.get(str(identifier))

        return None
