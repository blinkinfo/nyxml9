import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bot.keyboards import (
    settings_keyboard,
    threshold_action_name,
    threshold_bucket_action_keyboard,
    threshold_bucket_keyboard,
)
from bot.formatters import (
    format_threshold_bucket_browser,
    format_threshold_bucket_detail,
    format_threshold_controls_overview,
    format_threshold_help,
    format_threshold_policy_summary,
    format_threshold_recent_changes,
)


def test_threshold_bucket_keyboard_contains_bucket_callbacks():
    rows = [
        {'bucket': '0.50', 'action': 'default', 'resolved': 0, 'total': 0, 'win_pct': 0.0, 'is_hot': False, 'needs_review': False, 'configured': False},
        {'bucket': '0.51', 'action': 'follow', 'resolved': 2, 'total': 2, 'win_pct': 50.0, 'is_hot': False, 'needs_review': True, 'configured': True},
        {'bucket': '0.52', 'action': 'invert', 'resolved': 3, 'total': 3, 'win_pct': 66.7, 'is_hot': True, 'needs_review': False, 'configured': True},
    ]
    kb = threshold_bucket_keyboard('real', rows, filter_mode='configured', sort_mode='wr', offset=0, page_size=2)
    keys = [btn.callback_data for row in kb.inline_keyboard for btn in row]
    assert 'threshold_bucket_real_0.50_configured_wr_0' in keys
    assert 'threshold_bucket_real_0.51_configured_wr_0' in keys
    assert 'thresholds_browse_real_configured_wr_0' in keys
    assert max(len(key.encode('utf-8')) for key in keys) <= 64


def test_threshold_action_keyboard_contains_compact_actions_and_back_callback():
    kb = threshold_bucket_action_keyboard('demo', '0.58', back_callback='thresholds_browse_demo_hot_wr_8')
    actions = [btn.callback_data for row in kb.inline_keyboard for btn in row]
    assert 'threshold_set_demo_0.58_f_demo:hot:wr:8' in actions
    assert 'threshold_set_demo_0.58_i_demo:hot:wr:8' in actions
    assert 'threshold_set_demo_0.58_b_demo:hot:wr:8' in actions
    assert 'threshold_clear_demo_0.58_demo:hot:wr:8' in actions
    assert 'thresholds_browse_demo_hot_wr_8' in actions
    assert max(len(action.encode('utf-8')) for action in actions) <= 64


def test_threshold_action_name_accepts_compact_and_legacy_tokens():
    assert threshold_action_name('f') == 'follow'
    assert threshold_action_name('i') == 'invert'
    assert threshold_action_name('b') == 'block'
    assert threshold_action_name('follow') == 'follow'


def test_threshold_formatters_render_dashboard_browser_and_detail():
    overview = format_threshold_controls_overview('real', {
        'configured_count': 3,
        'active_buckets': 5,
        'resolved_count': 8,
        'skipped_count': 2,
        'win_rate': 62.5,
        'last_seen': '2025-01-01 00:00:00 UTC',
        'policy_mix': {'follow': 1, 'invert': 1, 'block': 1},
        'needs_review_count': 1,
        'observed_events': 10,
    }, [
        {'bucket': '0.58', 'action': 'invert', 'win_pct': 66.7, 'resolved': 3, 'last_seen': '2025-01-01 00:00:00 UTC', 'is_hot': True, 'needs_review': False}
    ])
    # New title format uses em-dash separator
    assert 'Threshold Dashboard' in overview
    assert 'REAL' in overview
    # Policy mix section uses emoji labels
    assert 'Follow' in overview
    assert 'Invert' in overview

    browser = format_threshold_bucket_browser('demo', 'review', 'recent', [
        {'bucket': '0.67', 'action': 'block', 'resolved': 0, 'total': 4, 'skipped_count': 4, 'win_pct': 0.0, 'is_hot': False, 'needs_review': True, 'configured': True}
    ], 0)
    assert 'Needs Review' in browser
    assert 'DEMO' in browser
    # Emoji-led rows: ⚠️ prefix for needs_review buckets, no plain 'REV' tag
    assert '0.67' in browser

    detail = format_threshold_bucket_detail({
        'bucket': '0.67',
        'channel': 'demo',
        'configured_action': 'block',
        'totals': {'resolved': 0, 'wins': 0, 'losses': 0, 'win_pct': 0.0, 'fired_count': 0, 'skipped_count': 4, 'avg_prob': 0.6791, 'last_seen': '2025-01-01 00:00:00 UTC'},
        'breakdown': [
            {'raw_side': 'Down', 'final_side': None, 'action': 'BLOCK', 'total': 4, 'win_pct': 0.0, 'avg_prob': 0.6791}
        ],
        'nearby': [
            {'bucket': '0.66', 'action': 'follow', 'win_pct': 55.0, 'total': 2}
        ],
        'recommendation': 'Lean BLOCK: bucket is mostly skipping or being suppressed.',
    })
    assert 'Bucket 0.67' in detail
    assert 'DEMO' in detail
    assert 'Blocked' in detail or 'BLOCKED' in detail
    # Note section replaces 'Operator note:' label
    assert 'Lean BLOCK' in detail


def test_threshold_summary_changes_and_help_formatters():
    summary = format_threshold_policy_summary('real', {
        'counts': {'follow': 1, 'invert': 2, 'block': 1},
        'rows': [
            {'bucket': '0.55', 'action': 'follow', 'win_pct': 60.0, 'total': 3, 'last_seen': '2025-01-01 00:00:00 UTC'}
        ],
    })
    changes = format_threshold_recent_changes('real', [
        {'bucket': '0.55', 'action': 'follow', 'updated_at': '2025-01-01 00:00:00 UTC'}
    ])
    help_text = format_threshold_help('demo')
    # New title format uses em-dash separator
    assert 'Policy Summary' in summary
    assert 'REAL' in summary
    assert 'Recent Changes' in changes
    assert 'REAL' in changes
    assert 'Help' in help_text
    assert 'DEMO' in help_text


def test_settings_keyboard_exposes_threshold_controls_entry():
    kb = settings_keyboard(True, 5.0)
    actions = [btn.callback_data for row in kb.inline_keyboard for btn in row]
    assert 'thresholds_home_real' in actions


def test_threshold_channel_keyboard_returns_to_settings():
    from bot.keyboards import threshold_channel_keyboard

    kb = threshold_channel_keyboard('real')
    actions = [btn.callback_data for row in kb.inline_keyboard for btn in row]
    assert 'cmd_settings' in actions
    assert 'thresholds_policy_real' in actions
    assert 'thresholds_help_real' in actions
