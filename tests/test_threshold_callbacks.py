import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bot.keyboards import threshold_bucket_action_keyboard, threshold_bucket_keyboard
from bot.formatters import format_threshold_bucket_detail, format_threshold_controls_overview


def test_threshold_bucket_keyboard_contains_bucket_callbacks():
    kb = threshold_bucket_keyboard('real', ['0.50', '0.51', '0.52'], offset=0, page_size=2)
    rows = kb.inline_keyboard
    assert rows[0][0].callback_data == 'threshold_bucket_real_0.50'
    assert rows[1][0].callback_data == 'threshold_bucket_real_0.51'


def test_threshold_action_keyboard_contains_all_actions():
    kb = threshold_bucket_action_keyboard('demo', '0.58')
    actions = [btn.callback_data for row in kb.inline_keyboard for btn in row]
    assert 'threshold_set_demo_0.58_follow' in actions
    assert 'threshold_set_demo_0.58_invert' in actions
    assert 'threshold_set_demo_0.58_block' in actions
    assert 'threshold_clear_demo_0.58' in actions


def test_threshold_formatters_render_expected_details():
    overview = format_threshold_controls_overview('real', [{'bucket': '0.58', 'action': 'invert'}], [
        {'bucket': '0.58', 'total': 3, 'skipped_count': 0, 'fired_count': 3, 'wins': 2, 'losses': 1, 'win_pct': 66.7, 'action_count': 1, 'raw_side_count': 1, 'final_side_count': 1}
    ])
    assert '0.58' in overview
    assert 'INVERT' in overview
    detail = format_threshold_bucket_detail('demo', '0.67', 'block', [
        {'raw_side': 'Down', 'final_side': None, 'action': 'BLOCK', 'total': 4, 'skipped_count': 4, 'wins': 0, 'losses': 0, 'win_pct': 0.0, 'avg_prob': 0.6791}
    ])
    assert 'Bucket 0.67 (DEMO)' in detail
    assert 'BLOCKED' in detail
