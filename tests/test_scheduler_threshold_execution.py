import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core import scheduler


def test_check_and_trade_executes_threshold_policy_for_fired_ml_signal(monkeypatch):
    sent_messages = []
    inserted_signals = []
    scheduled_jobs = []

    signal = {
        "skipped": False,
        "side": "Down",
        "entry_price": 0.62,
        "opposite_price": 0.38,
        "token_id": "down-token",
        "opposite_token_id": "up-token",
        "pattern": None,
        "slot_n1_start_full": "2026-04-24 18:10",
        "slot_n1_end_full": "2026-04-24 18:15",
        "slot_n1_start_str": "18:10",
        "slot_n1_end_str": "18:15",
        "slot_n1_ts": 1777054200,
        "slot_n1_slug": "btc-updown-5m-1777054200",
        "ml_p_up": 0.37748564,
        "ml_p_down": 0.622514,
        "ml_up_threshold": 0.505,
        "ml_down_threshold": 0.505,
        "ml_down_enabled": True,
    }

    async def fake_check_signal():
        return signal

    async def fake_send(text: str):
        sent_messages.append(text)

    async def fake_is_demo_trade_enabled():
        return False

    async def fake_is_invert_trades_enabled():
        return False

    async def fake_get_threshold_control(channel: str, bucket: str):
        assert channel == "real"
        assert bucket == str(signal["ml_p_down"])
        return None

    async def fake_insert_signal(**kwargs):
        inserted_signals.append(kwargs)
        return 123

    class DummyTradeManager:
        @staticmethod
        async def check(**kwargs):
            return SimpleNamespace(allowed=True)

    async def fake_is_autotrade_enabled():
        return False

    async def fake_resolve_trade_amount(poly_client=None, is_demo=False):
        assert is_demo is False
        return 5.0, "$5.00 (fixed)"

    class DummyScheduler:
        def add_job(self, func, trigger, run_date, kwargs, id, replace_existing):
            scheduled_jobs.append(
                {
                    "func": func,
                    "trigger": trigger,
                    "run_date": run_date,
                    "kwargs": kwargs,
                    "id": id,
                    "replace_existing": replace_existing,
                }
            )

    monkeypatch.setattr(scheduler.strategy, "check_signal", fake_check_signal)
    monkeypatch.setattr(scheduler, "_send_telegram", fake_send)
    monkeypatch.setattr(scheduler.queries, "is_demo_trade_enabled", fake_is_demo_trade_enabled)
    monkeypatch.setattr(scheduler.queries, "is_invert_trades_enabled", fake_is_invert_trades_enabled)
    monkeypatch.setattr(scheduler.queries, "get_threshold_control", fake_get_threshold_control)
    monkeypatch.setattr(scheduler.queries, "insert_signal", fake_insert_signal)
    monkeypatch.setattr(scheduler.queries, "is_autotrade_enabled", fake_is_autotrade_enabled)
    monkeypatch.setattr(scheduler.queries, "resolve_trade_amount", fake_resolve_trade_amount)
    monkeypatch.setattr(scheduler, "SCHEDULER", DummyScheduler())
    monkeypatch.setattr(scheduler, "_schedule_next", lambda: None)

    import core.trade_manager as trade_manager

    monkeypatch.setattr(trade_manager, "TradeManager", DummyTradeManager)

    asyncio.run(scheduler._check_and_trade())

    assert len(inserted_signals) == 1
    stored = inserted_signals[0]
    assert stored["raw_side"] == "Down"
    assert stored["final_side"] == "Down"
    assert stored["threshold_bucket"] == "0.62"
    assert stored["threshold_action"] == "FOLLOW"
    assert stored["threshold_channel"] == "real"
    assert stored["threshold_source"] == "default_follow"
    assert stored["policy_note"] == "raw=Down final=Down bucket=0.62 action=FOLLOW"

    assert len(sent_messages) == 1
    assert "Signal Fired" in sent_messages[0]
    assert "Policy: Down -> Down  (FOLLOW)" in sent_messages[0]
    assert "Bucket: 0.62  |  Channel: REAL" in sent_messages[0]

    assert len(scheduled_jobs) == 1
    assert scheduled_jobs[0]["id"] == "resolve_123"
    assert scheduled_jobs[0]["kwargs"]["signal_id"] == 123
    assert scheduled_jobs[0]["kwargs"]["side"] == "Down"
    assert scheduled_jobs[0]["kwargs"]["trade_id"] is None
    assert scheduled_jobs[0]["kwargs"]["amount_usdc"] is None
