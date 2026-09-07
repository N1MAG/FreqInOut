from __future__ import annotations

from dataclasses import dataclass


class MeshOperationCancelled(RuntimeError):
    pass


@dataclass(frozen=True)
class MeshOperationSnapshot:
    operation_id: str
    source_id: str
    request_class: str
    state: str
    progress_current: int = 0
    progress_total: int = 0
    started_monotonic_ms: int = 0
    completed_monotonic_ms: int = 0
    elapsed_ms: int = 0
    supersedes: str = ""
    detail: str = ""


@dataclass(frozen=True)
class MeshRetryPolicy:
    initial_delay_ms: int = 15_000
    maximum_delay_ms: int = 300_000
    multiplier: float = 2.0

    def delay_ms(self, failure_count: int) -> int:
        failures = max(1, int(failure_count))
        delay = float(max(250, self.initial_delay_ms)) * (max(1.0, self.multiplier) ** (failures - 1))
        return min(max(250, self.maximum_delay_ms), int(delay))


@dataclass
class MeshRetryState:
    failure_count: int = 0
    next_retry_ms: int = 0
    operator_action_required: bool = False

    def due(self, now_ms: int) -> bool:
        if self.operator_action_required:
            return False
        return self.next_retry_ms <= 0 or int(now_ms) >= self.next_retry_ms

    def record_failure(self, now_ms: int, policy: MeshRetryPolicy) -> int:
        self.operator_action_required = False
        self.failure_count += 1
        delay = policy.delay_ms(self.failure_count)
        self.next_retry_ms = int(now_ms) + delay
        return delay

    def record_operator_action_required(self) -> None:
        self.failure_count += 1
        self.next_retry_ms = 0
        self.operator_action_required = True

    def record_success(self) -> None:
        self.failure_count = 0
        self.next_retry_ms = 0
        self.operator_action_required = False

    def retry_now(self) -> None:
        self.next_retry_ms = 0
        self.operator_action_required = False

    def remaining_ms(self, now_ms: int) -> int:
        return max(0, self.next_retry_ms - int(now_ms))


def mesh_error_requires_operator_action(error: object) -> bool:
    """Return whether retries cannot repair the reported BLE security state."""

    text = str(error or "").casefold()
    return any(
        marker in text
        for marker in (
            "peer removed pairing information",
            "card removed its saved bluetooth pairing information",
            "cberrordomain code=14",
        )
    )
