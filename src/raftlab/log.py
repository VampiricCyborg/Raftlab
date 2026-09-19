"""The replicated log.

Indexing is 1-based, as in the Raft paper. Index 0 is a virtual sentinel with
term 0, so an AppendEntries with ``prev_log_index=0, prev_log_term=0`` always
matches: that is how a leader says "start from the very beginning".
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class LogEntry:
    term: int
    command: str


class RaftLog:
    def __init__(self, entries: Iterable[LogEntry] = ()) -> None:
        self._entries: list[LogEntry] = list(entries)

    def __len__(self) -> int:
        return len(self._entries)

    def __repr__(self) -> str:
        return f"RaftLog({[e.term for e in self._entries]})"

    @property
    def last_index(self) -> int:
        return len(self._entries)

    @property
    def last_term(self) -> int:
        """Term of the last entry, or 0 for an empty log."""
        return self._entries[-1].term if self._entries else 0

    def term_at(self, index: int) -> int | None:
        """Term of the entry at ``index``; 0 for the sentinel; None if absent."""
        if index == 0:
            return 0
        if 1 <= index <= len(self._entries):
            return self._entries[index - 1].term
        return None

    def entry(self, index: int) -> LogEntry:
        if not 1 <= index <= len(self._entries):
            raise IndexError(f"no log entry at index {index} (last={self.last_index})")
        return self._entries[index - 1]

    def entries_from(self, index: int) -> tuple[LogEntry, ...]:
        """All entries at ``index`` and after (empty tuple past the end)."""
        return tuple(self._entries[max(index, 1) - 1 :])

    def entries(self) -> tuple[LogEntry, ...]:
        return tuple(self._entries)

    def append(self, entry: LogEntry) -> int:
        """Append one entry and return its index."""
        self._entries.append(entry)
        return len(self._entries)

    def truncate_from(self, index: int) -> None:
        """Delete the entry at ``index`` and everything after it."""
        if index < 1:
            raise ValueError("cannot truncate the sentinel at index 0")
        del self._entries[index - 1 :]
