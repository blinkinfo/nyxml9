"""Threshold bucket policy resolution for ML signals."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Literal

PolicyAction = Literal["follow", "invert", "block"]
ExecutionChannel = Literal["demo", "real"]


@dataclass(frozen=True)
class ThresholdPolicyDecision:
    channel: ExecutionChannel
    raw_side: str
    final_side: str | None
    bucket: str
    bucket_probability: float
    action: PolicyAction
    source: str
    display_action: str
    blocked: bool


_VALID_SIDES = {"Up", "Down"}
_ACTION_TO_DISPLAY = {
    "follow": "FOLLOW",
    "invert": "INVERT",
    "block": "BLOCK",
}


def truncate_probability_bucket(probability: float) -> str:
    """Truncate a probability to a two-decimal bucket without rounding."""
    dec = Decimal(str(probability)).quantize(Decimal("0.00"), rounding=ROUND_DOWN)
    if dec < Decimal("0.00"):
        dec = Decimal("0.00")
    if dec > Decimal("1.00"):
        dec = Decimal("1.00")
    return format(dec, ".2f")


def choose_bucket_probability(raw_side: str, p_up: float, p_down: float) -> float:
    """Use p_up for Up raw signals and p_down for Down raw signals."""
    if raw_side not in _VALID_SIDES:
        raise ValueError(f"Unsupported raw_side: {raw_side}")
    return float(p_up if raw_side == "Up" else p_down)


def invert_side(side: str) -> str:
    if side == "Up":
        return "Down"
    if side == "Down":
        return "Up"
    raise ValueError(f"Unsupported side: {side}")


def resolve_threshold_policy(
    *,
    channel: ExecutionChannel,
    raw_side: str,
    p_up: float,
    p_down: float,
    bucket_action: str | None,
    default_action: str = "follow",
    default_source: str = "default",
) -> ThresholdPolicyDecision:
    """Resolve final execution behavior for a raw signal."""
    bucket_probability = choose_bucket_probability(raw_side, p_up, p_down)
    bucket = truncate_probability_bucket(bucket_probability)
    normalized = (bucket_action or default_action or "follow").strip().lower()
    if normalized not in {"follow", "invert", "block"}:
        normalized = default_action.strip().lower() if default_action else "follow"
    source = "bucket" if bucket_action else default_source

    if normalized == "block":
        return ThresholdPolicyDecision(
            channel=channel,
            raw_side=raw_side,
            final_side=None,
            bucket=bucket,
            bucket_probability=bucket_probability,
            action="block",
            source=source,
            display_action=_ACTION_TO_DISPLAY["block"],
            blocked=True,
        )

    final_side = raw_side if normalized == "follow" else invert_side(raw_side)
    return ThresholdPolicyDecision(
        channel=channel,
        raw_side=raw_side,
        final_side=final_side,
        bucket=bucket,
        bucket_probability=bucket_probability,
        action=normalized,
        source=source,
        display_action=_ACTION_TO_DISPLAY[normalized],
        blocked=False,
    )
