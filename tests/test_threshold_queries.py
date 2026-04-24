import asyncio
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config as cfg
from db import models, queries


def test_threshold_controls_persist_and_aggregate():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / 'test.db')
        original = cfg.DB_PATH
        cfg.DB_PATH = db_path
        try:
            asyncio.run(models.init_db(db_path))
            asyncio.run(models.migrate_db(db_path))
            asyncio.run(queries.set_threshold_control('real', '0.589', 'invert'))
            row = asyncio.run(queries.get_threshold_control('real', '0.58'))
            assert row is not None
            assert row['bucket'] == '0.58'
            assert row['action'] == 'invert'

            asyncio.run(queries.insert_signal(
                slot_start='2025-01-01 00:00:00 UTC',
                slot_end='2025-01-01 00:05:00 UTC',
                slot_timestamp=1,
                side='Down',
                entry_price=0.41,
                opposite_price=0.59,
                skipped=False,
                pattern='p_up=0.5890,p_down=0.4110',
                raw_side='Up',
                final_side='Down',
                threshold_bucket='0.58',
                threshold_action='INVERT',
                threshold_channel='real',
                threshold_source='bucket',
                threshold_bucket_prob=0.589,
                policy_note='raw=Up final=Down bucket=0.58 action=INVERT',
            ))
            asyncio.run(queries.resolve_signal(1, 'Down', True))
            stats = asyncio.run(queries.get_signal_stats())
            assert stats['policy_blocked_count'] == 0
            bucket_stats = asyncio.run(queries.get_threshold_bucket_stats('real'))
            assert len(bucket_stats) == 1
            assert bucket_stats[0]['bucket'] == '0.58'
            assert bucket_stats[0]['win_pct'] == 100.0
        finally:
            cfg.DB_PATH = original
