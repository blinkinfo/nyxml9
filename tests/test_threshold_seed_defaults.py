import asyncio
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config as cfg
from db import models, queries


def test_init_and_migrate_seed_same_default_thresholds():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / 'test.db')
        original = cfg.DB_PATH
        cfg.DB_PATH = db_path
        try:
            asyncio.run(models.init_db(db_path))
            up_after_init = asyncio.run(queries.get_ml_threshold())
            down_after_init = asyncio.run(queries.get_ml_down_threshold())
            assert up_after_init == cfg.ML_DEFAULT_THRESHOLD
            assert down_after_init == round(1.0 - cfg.ML_DEFAULT_THRESHOLD, 4)

            asyncio.run(models.migrate_db(db_path))
            up_after_migrate = asyncio.run(queries.get_ml_threshold())
            down_after_migrate = asyncio.run(queries.get_ml_down_threshold())
            assert up_after_migrate == cfg.ML_DEFAULT_THRESHOLD
            assert down_after_migrate == round(1.0 - cfg.ML_DEFAULT_THRESHOLD, 4)
        finally:
            cfg.DB_PATH = original
