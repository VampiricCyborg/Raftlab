"""The toy state machine: a dict that understands ``SET key=value``. That's it."""

from __future__ import annotations


class KVStateMachine:
    def __init__(self) -> None:
        self.data: dict[str, str] = {}
        # Append-only record of every (index, command) this node has applied.
        # It is instrumentation for invariant I5, not node state, so it
        # survives restarts: a restarted node re-applies from index 1 and the
        # repeats must agree with what it applied before.
        self.history: list[tuple[int, str]] = []

    def apply(self, index: int, command: str) -> None:
        self.history.append((index, command))
        op, _, arg = command.partition(" ")
        if op == "SET":
            key, _, value = arg.partition("=")
            self.data[key] = value
        elif op != "NOOP":
            raise ValueError(f"unknown command: {command!r}")

    def reset(self) -> None:
        """Crash semantics: the applied state is volatile and rebuilt from the log."""
        self.data.clear()
