"""Inline keyboard layouts for the Telegram bot."""

from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


# ---------------------------------------------------------------------------
# Consistent filter button helper
# ---------------------------------------------------------------------------

def _filter_btn(label: str, callback_data: str, active: str) -> InlineKeyboardButton:
    """Return a filter tab button with ✅ prefix when active."""
    prefix = "✅ " if callback_data.endswith(f"_{active}") else ""
    return InlineKeyboardButton(f"{prefix}{label}", callback_data=callback_data)


# ---------------------------------------------------------------------------
# Main menu
# ---------------------------------------------------------------------------

def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\U0001f4ca Status", callback_data="cmd_status"),
            InlineKeyboardButton("\U0001f4e1 Signals", callback_data="cmd_signals"),
        ],
        [
            InlineKeyboardButton("\U0001f4b9 Trades", callback_data="cmd_trades"),
            InlineKeyboardButton("\U0001f9e9 Patterns", callback_data="cmd_patterns"),
        ],
        [
            InlineKeyboardButton("\U0001f4b8 Redeem", callback_data="cmd_redeem"),
            InlineKeyboardButton("\U0001f4dc Redemptions", callback_data="cmd_redemptions"),
        ],
        [
            InlineKeyboardButton("\u2699\ufe0f Settings", callback_data="cmd_settings"),
            InlineKeyboardButton("\U0001f9ea Demo", callback_data="cmd_demo"),
        ],
        [
            InlineKeyboardButton("\U0001f916 ML Model", callback_data="cmd_ml"),
        ],
        [
            InlineKeyboardButton("\u2753 Help", callback_data="cmd_help"),
            InlineKeyboardButton("\U0001f3e0 Home", callback_data="cmd_menu"),
        ],
    ])


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

def settings_keyboard(
    autotrade_on: bool,
    trade_amount: float,
    auto_redeem_on: bool = False,
    demo_trade_on: bool = False,
    demo_bankroll: float = 1000.00,
    trade_mode: str = "fixed",
    trade_pct: float = 5.0,
    invert_trades_on: bool = False,
    ml_volatility_gate_enabled: bool = True,
) -> InlineKeyboardMarkup:
    # Row 1: paired toggles — related switches side-by-side
    at_label = f"\U0001f916 AutoTrade: {'ON' if autotrade_on else 'OFF'}"
    ar_label = f"{'\U0001f4b0' if auto_redeem_on else '\U0001f4e6'} Auto-Redeem: {'ON' if auto_redeem_on else 'OFF'}"

    # Row 2: mode toggle + value input
    if trade_mode == "pct":
        mode_label = "\U0001f4ca Mode: PCT"
        value_label = f"\U0001f4b5 {trade_pct:.1f}%"
    else:
        mode_label = "\U0001f4ca Mode: FIXED"
        value_label = f"\U0001f4b5 ${trade_amount:.2f}"

    # Row 3: demo toggle + bankroll display
    dt_label = f"\U0001f9ea Demo: {'ON' if demo_trade_on else 'OFF'}"
    db_label = f"\U0001f4b5 Bankroll: ${demo_bankroll:.2f}"

    return InlineKeyboardMarkup([
        # Paired toggles
        [
            InlineKeyboardButton(at_label, callback_data="toggle_autotrade"),
            InlineKeyboardButton(ar_label, callback_data="toggle_auto_redeem"),
        ],
        # Mode + value (side by side)
        [
            InlineKeyboardButton(mode_label, callback_data="toggle_trade_mode"),
            InlineKeyboardButton(value_label, callback_data="change_amount"),
        ],
        # Demo section
        [
            InlineKeyboardButton(dt_label, callback_data="toggle_demo_trade"),
            InlineKeyboardButton(db_label, callback_data="set_demo_bankroll"),
        ],
        # Destructive action — full-width, alone
        [InlineKeyboardButton("\U0001f504 Reset Bankroll", callback_data="reset_demo_bankroll")],
        # Invert Trades
        [InlineKeyboardButton(
            f"\U0001f504 Invert Trades: {'ON' if invert_trades_on else 'OFF'}",
            callback_data="toggle_invert_trades",
        )],
        # ML volatility gate
        [InlineKeyboardButton(
            f"\U0001f6e1 ML Volatility Gate: {'ON' if ml_volatility_gate_enabled else 'OFF'}",
            callback_data="toggle_ml_volatility_gate",
        )],
        # Threshold controls
        [InlineKeyboardButton("\U0001f3af Threshold Controls", callback_data="thresholds_home_real")],
        # Back
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


# ---------------------------------------------------------------------------
# Filter rows (Last 10 / Last 50 / All Time)
# ---------------------------------------------------------------------------

def signal_filter_row(active: str = "all") -> InlineKeyboardMarkup:
    buttons = [
        _filter_btn("Last 10", "signals_10", active),
        _filter_btn("Last 50", "signals_50", active),
        _filter_btn("All Time", "signals_all", active),
    ]
    return InlineKeyboardMarkup([
        buttons,
        [
            InlineKeyboardButton("\U0001f4c4 CSV", callback_data="download_csv"),
            InlineKeyboardButton("\U0001f4ca Excel", callback_data="download_xlsx"),
        ],
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


def trade_filter_row(active: str = "all") -> InlineKeyboardMarkup:
    buttons = [
        _filter_btn("Last 10", "trades_10", active),
        _filter_btn("Last 50", "trades_50", active),
        _filter_btn("All Time", "trades_all", active),
    ]
    return InlineKeyboardMarkup([
        buttons,
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


# ---------------------------------------------------------------------------
# Back button only
# ---------------------------------------------------------------------------

def back_to_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


# ---------------------------------------------------------------------------
# Download keyboard (standalone)
# ---------------------------------------------------------------------------

def download_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\U0001f4c4 CSV", callback_data="download_csv"),
            InlineKeyboardButton("\U0001f4ca Excel", callback_data="download_xlsx"),
        ],
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


# ---------------------------------------------------------------------------
# Redeem keyboards
# ---------------------------------------------------------------------------

def redeem_confirm_keyboard() -> InlineKeyboardMarkup:
    """Shown after a dry-run scan — lets user confirm or cancel."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("\u2705 Confirm & Redeem All", callback_data="redeem_confirm")],
        [
            InlineKeyboardButton("\u274c Cancel", callback_data="redeem_cancel"),
            InlineKeyboardButton("\U0001f519 Menu", callback_data="cmd_menu"),
        ],
    ])


def redeem_done_keyboard() -> InlineKeyboardMarkup:
    """Shown after redemptions complete."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\U0001f4dc History", callback_data="cmd_redemptions"),
            InlineKeyboardButton("\U0001f519 Menu", callback_data="cmd_menu"),
        ],
    ])


def demo_filter_row(active: str = "all") -> InlineKeyboardMarkup:
    """Filter row for the /demo dashboard."""
    return InlineKeyboardMarkup([
        [
            _filter_btn("Last 10", "demo_10", active),
            _filter_btn("Last 50", "demo_50", active),
            _filter_btn("All Time", "demo_all", active),
        ],
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


def pattern_filter_row() -> InlineKeyboardMarkup:
    """Keyboard for the /patterns dashboard — kept for backward compat."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\U0001f4ca Excel", callback_data="download_pattern_xlsx"),
        ],
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


# ---------------------------------------------------------------------------
# Pattern performance keyboard
# ---------------------------------------------------------------------------

def pattern_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for the /patterns dashboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("\U0001f4ca Excel", callback_data="download_pattern_xlsx")],
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


# ---------------------------------------------------------------------------
# ML Model submenu
# ---------------------------------------------------------------------------

def down_override_keyboard() -> InlineKeyboardMarkup:
    """Shown after an auto-promote (or force-promote) when the DOWN side failed
    its own 59 % gate.

    The UP model has already been promoted to current.  The user now chooses
    whether to enable the DOWN signal anyway or to leave it disabled.
    """
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\u26a0\ufe0f Enable DOWN Anyway", callback_data="ml_down_override_anyway"),
            InlineKeyboardButton("\u274c Keep DOWN Disabled", callback_data="ml_down_override_skip"),
        ],
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


def retrain_blocked_keyboard() -> InlineKeyboardMarkup:
    """Shown after a retrain that failed the 59% deployment gate.

    Candidate is already saved — user picks Promote Anyway or Discard.
    """
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\u26a0\ufe0f Promote Anyway", callback_data="ml_promote_anyway"),
            InlineKeyboardButton("\U0001f5d1 Discard Candidate", callback_data="ml_discard_candidate"),
        ],
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


def ml_menu() -> InlineKeyboardMarkup:
    """Inline keyboard for the ML Model submenu."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\U0001f4ca Model Status", callback_data="ml_status"),
            InlineKeyboardButton("\U0001f4cf Compare Models", callback_data="ml_compare"),
        ],
        [
            InlineKeyboardButton("\u2b06\ufe0f Promote Candidate", callback_data="ml_promote"),
            InlineKeyboardButton("\U0001f504 Retrain", callback_data="ml_retrain"),
        ],
        [
            InlineKeyboardButton("\u2699\ufe0f Set UP Threshold", callback_data="ml_set_threshold"),
            InlineKeyboardButton("\u2699\ufe0f Set DOWN Threshold", callback_data="ml_set_down_threshold"),
        ],
        [InlineKeyboardButton("\U0001f519 Back to Menu", callback_data="cmd_menu")],
    ])


def ml_volatility_gate_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Disable Gate", callback_data="confirm_disable_ml_volatility_gate")],
        [InlineKeyboardButton("Keep Gate Enabled", callback_data="cancel_disable_ml_volatility_gate")],
        [InlineKeyboardButton("Back to Settings", callback_data="cmd_settings")],
    ])


def threshold_channel_keyboard(active: str = "real") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            _filter_btn("\u2705 REAL" if active == "real" else "REAL", "thresholds_home_real", active),
            _filter_btn("\u2705 DEMO" if active == "demo" else "DEMO", "thresholds_home_demo", active),
        ],
        [
            InlineKeyboardButton("\U0001f5c2\ufe0f All Buckets", callback_data=f"thresholds_browse_{active}_all_bucket_0"),
            InlineKeyboardButton("Overrides Only", callback_data=f"thresholds_browse_{active}_configured_bucket_0"),
        ],
        [
            InlineKeyboardButton("\U0001f525 Hot Buckets", callback_data=f"thresholds_browse_{active}_hot_wr_0"),
            InlineKeyboardButton("\u26a0\ufe0f Needs Review", callback_data=f"thresholds_browse_{active}_review_recent_0"),
        ],
        [
            InlineKeyboardButton("\U0001f4cb Policy Summary", callback_data=f"thresholds_policy_{active}"),
            InlineKeyboardButton("\U0001f501 Changes", callback_data=f"thresholds_changes_{active}"),
        ],
        [InlineKeyboardButton("\U0001f4a1 Help", callback_data=f"thresholds_help_{active}")],
        [InlineKeyboardButton("\u2190 Settings", callback_data="cmd_settings")],
    ])


def threshold_bucket_keyboard(channel: str, buckets: list[dict], filter_mode: str = 'all', sort_mode: str = 'bucket', offset: int = 0, page_size: int = 8) -> InlineKeyboardMarkup:
    page = buckets[offset:offset + page_size]
    rows = []
    for row in page:
        # Emoji status icon
        if row.get('is_hot'):
            icon = '\U0001f525'  # 🔥
        elif row.get('needs_review'):
            icon = '\u26a0\ufe0f'  # ⚠️
        else:
            action = str(row.get('action') or '').lower()
            if action == 'follow':
                icon = '\U0001f7e2'  # 🟢
            elif action == 'invert':
                icon = '\U0001f504'  # 🔄
            elif action == 'block':
                icon = '\U0001f534'  # 🔴
            else:
                icon = '\u2b55'  # ⭕
        win   = f"{row.get('win_pct', 0.0):.0f}%" if row.get('resolved', 0) else '--'
        total = int(row.get('total', 0) or 0)
        action_label = str(row.get('action') or 'default').upper()
        label = f"{icon} {row['bucket']}  {action_label}  {win}  {total}"
        rows.append([InlineKeyboardButton(label.strip(), callback_data=f"threshold_bucket_{channel}_{row['bucket']}_{filter_mode}_{sort_mode}_{offset}")])

    # Filter tabs — mark active with checkmark
    def _fb(label: str, cb: str, active_val: str, current_val: str) -> InlineKeyboardButton:
        prefix = '\u2705 ' if current_val == active_val else ''
        return InlineKeyboardButton(f"{prefix}{label}", callback_data=cb)

    filter_row = [
        _fb('All',      f'thresholds_browse_{channel}_all_{sort_mode}_0',        'all',        filter_mode),
        _fb('Overrides', f'thresholds_browse_{channel}_configured_{sort_mode}_0', 'configured', filter_mode),
        _fb('\U0001f525 Hot',  f'thresholds_browse_{channel}_hot_{sort_mode}_0',       'hot',        filter_mode),
        _fb('\u26a0\ufe0f Rev', f'thresholds_browse_{channel}_review_{sort_mode}_0',    'review',     filter_mode),
    ]
    sort_row = [
        _fb('Bucket', f'thresholds_browse_{channel}_{filter_mode}_bucket_0',   'bucket',   sort_mode),
        _fb('WR',     f'thresholds_browse_{channel}_{filter_mode}_wr_0',        'wr',       sort_mode),
        _fb('Recent', f'thresholds_browse_{channel}_{filter_mode}_recent_0',    'recent',   sort_mode),
        _fb('Volume', f'thresholds_browse_{channel}_{filter_mode}_activity_0',  'activity', sort_mode),
    ]
    rows.append(filter_row)
    rows.append(sort_row)

    nav = []
    if offset > 0:
        nav.append(InlineKeyboardButton('\u2190 Prev', callback_data=f'thresholds_browse_{channel}_{filter_mode}_{sort_mode}_{max(0, offset - page_size)}'))
    if offset + page_size < len(buckets):
        nav.append(InlineKeyboardButton('Next \u2192', callback_data=f'thresholds_browse_{channel}_{filter_mode}_{sort_mode}_{offset + page_size}'))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton('\u2190 Dashboard', callback_data=f'thresholds_home_{channel}')])
    return InlineKeyboardMarkup(rows)


def threshold_bucket_action_keyboard(channel: str, bucket: str, back_callback: str | None = None) -> InlineKeyboardMarkup:
    # Determine adjacent buckets for navigation (best effort from bucket string)
    try:
        lo, hi = bucket.split('-')
        step = round(float(hi) - float(lo), 4)
        prev_lo = round(float(lo) - step, 4)
        prev_hi = round(float(lo), 4)
        next_lo = round(float(hi), 4)
        next_hi = round(float(hi) + step, 4)
        prev_bucket = f"{prev_lo:.2f}-{prev_hi:.2f}"
        next_bucket = f"{next_lo:.2f}-{next_hi:.2f}"
        nav_row = [
            InlineKeyboardButton(f'\u2190 {prev_bucket}', callback_data=f'threshold_bucket_{channel}_{prev_bucket}_{back_callback or "all_bucket_0"}'),
            InlineKeyboardButton(f'{next_bucket} \u2192', callback_data=f'threshold_bucket_{channel}_{next_bucket}_{back_callback or "all_bucket_0"}'),
        ]
        has_nav = True
    except Exception:
        has_nav = False

    kb_rows = [
        [
            InlineKeyboardButton('\U0001f7e2 Set FOLLOW', callback_data=f'threshold_set_{channel}_{bucket}_follow'),
            InlineKeyboardButton('\U0001f504 Set INVERT', callback_data=f'threshold_set_{channel}_{bucket}_invert'),
        ],
        [
            InlineKeyboardButton('\U0001f534 Set BLOCK',  callback_data=f'threshold_set_{channel}_{bucket}_block'),
            InlineKeyboardButton('Clear Override',        callback_data=f'threshold_clear_{channel}_{bucket}'),
        ],
    ]
    if has_nav:
        kb_rows.append(nav_row)
    kb_rows.append([InlineKeyboardButton('\u2190 Back to list', callback_data=back_callback or f'thresholds_browse_{channel}_all_bucket_0')])
    return InlineKeyboardMarkup(kb_rows)
