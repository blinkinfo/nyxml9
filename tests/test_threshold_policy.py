import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.threshold_policy import (
    truncate_probability_bucket,
    resolve_threshold_policy,
)


def test_truncate_probability_bucket_without_rounding():
    assert truncate_probability_bucket(0.589) == "0.58"
    assert truncate_probability_bucket(0.581) == "0.58"
    assert truncate_probability_bucket(0.5) == "0.50"
    assert truncate_probability_bucket(1.0) == "1.00"


def test_policy_follow_uses_p_up_for_up_signal():
    decision = resolve_threshold_policy(
        channel="real",
        raw_side="Up",
        p_up=0.589,
        p_down=0.411,
        bucket_action="follow",
    )
    assert decision.bucket == "0.58"
    assert decision.final_side == "Up"
    assert decision.action == "follow"
    assert decision.blocked is False


def test_policy_invert_uses_p_down_for_down_signal():
    decision = resolve_threshold_policy(
        channel="demo",
        raw_side="Down",
        p_up=0.321,
        p_down=0.679,
        bucket_action="invert",
    )
    assert decision.bucket == "0.67"
    assert decision.final_side == "Up"
    assert decision.action == "invert"


def test_policy_block_returns_no_final_side():
    decision = resolve_threshold_policy(
        channel="real",
        raw_side="Down",
        p_up=0.15,
        p_down=0.85,
        bucket_action="block",
    )
    assert decision.final_side is None
    assert decision.blocked is True
    assert decision.display_action == "BLOCK"


def test_default_action_can_follow_global_invert():
    decision = resolve_threshold_policy(
        channel="real",
        raw_side="Up",
        p_up=0.77,
        p_down=0.23,
        bucket_action=None,
        default_action="invert",
        default_source="global_invert_default",
    )
    assert decision.final_side == "Down"
    assert decision.source == "global_invert_default"
