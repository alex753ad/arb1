"""
tests/unit/test_types_utils.py — Тесты для core/types.py и core/utils.py
"""

import pytest
import os
import json
import tempfile
from pairs_scanner.core.types import Position, ScanConfig, PairResult, RiskSizingResult
from pairs_scanner.core.utils import (
    now_msk, MSK, today_msk_str, atomic_json_save,
    calc_pair_pnl, COMMISSION_ROUND_TRIP_PCT,
)


class TestPosition:
    def test_from_dict_ignores_unknown_keys(self):
        """Forward compat: неизвестные ключи не ломают from_dict."""
        d = {
            'id': 1, 'coin1': 'ETH', 'coin2': 'BTC',
            'direction': 'LONG', 'status': 'OPEN',
            'future_field_v50': 'some_value',  # unknown key
        }
        pos = Position.from_dict(d)
        assert pos.id == 1
        assert pos.coin1 == 'ETH'
        assert pos.direction == 'LONG'

    def test_to_dict_round_trip(self):
        pos = Position(id=1, coin1='ETH', coin2='BTC', direction='LONG')
        d = pos.to_dict()
        pos2 = Position.from_dict(d)
        assert pos2.id == pos.id
        assert pos2.coin1 == pos.coin1
        assert pos2.direction == pos.direction

    def test_to_dict_preserves_key_fields(self):
        """Ключевые поля сохраняются даже если default."""
        pos = Position(id=0, coin1='A', coin2='B', direction='SHORT')
        d = pos.to_dict()
        assert 'id' in d
        assert 'entry_time' in d
        assert 'pnl_pct' in d
        assert 'auto_opened' in d

    def test_defaults(self):
        pos = Position(id=1, coin1='ETH', coin2='BTC', direction='LONG')
        assert pos.status == 'OPEN'
        assert pos.timeframe == '4h'
        assert pos.pnl_pct == 0.0
        assert pos.auto_opened is False

    def test_trailing_state_defaults(self):
        pos = Position(id=1, coin1='A', coin2='B', direction='LONG')
        assert pos.exit_phase == 1
        assert pos._z_trail_activated is False
        assert pos._tp_trail_peak == 0.0


class TestScanConfig:
    def test_defaults(self):
        cfg = ScanConfig()
        assert cfg.timeframe == '4h'
        assert cfg.min_quality == 65
        assert cfg.max_halflife_hours == 28.0

    def test_override(self):
        cfg = ScanConfig(timeframe='1h', min_quality=70)
        assert cfg.timeframe == '1h'
        assert cfg.min_quality == 70


class TestNowMsk:
    def test_returns_aware_datetime(self):
        dt = now_msk()
        assert dt.tzinfo is not None
        assert dt.tzinfo == MSK

    def test_today_str_format(self):
        s = today_msk_str()
        assert len(s) == 10  # YYYY-MM-DD
        assert s[4] == '-'


class TestAtomicJsonSave:
    def test_writes_valid_json(self):
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            path = f.name
        try:
            data = {'key': 'value', 'num': 42}
            result = atomic_json_save(path, data)
            assert result is True
            with open(path) as f:
                loaded = json.load(f)
            assert loaded == data
        finally:
            os.unlink(path)

    def test_overwrites_existing(self):
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False, mode='w') as f:
            json.dump({'old': True}, f)
            path = f.name
        try:
            atomic_json_save(path, {'new': True})
            with open(path) as f:
                loaded = json.load(f)
            assert loaded == {'new': True}
        finally:
            os.unlink(path)


class TestCalcPairPnl:
    def test_long_profit_when_spread_narrows(self):
        """LONG: profit when coin1 drops, coin2 rises."""
        pnl = calc_pair_pnl(
            direction='LONG',
            entry_price1=100, entry_price2=50,
            exit_price1=95, exit_price2=55,
            entry_hr=1.0,
            commission_pct=0,
        )
        # ret1 = -5%, ret2 = +10% → raw = (-(-5%) + 1.0*(+10%)) / 2 = 7.5%
        assert pnl > 0

    def test_short_profit_when_spread_widens(self):
        pnl = calc_pair_pnl(
            direction='SHORT',
            entry_price1=100, entry_price2=50,
            exit_price1=110, exit_price2=45,
            entry_hr=1.0,
            commission_pct=0,
        )
        assert pnl > 0

    def test_commission_reduces_pnl(self):
        pnl_no_comm = calc_pair_pnl('LONG', 100, 50, 95, 55, 1.0, commission_pct=0)
        pnl_with_comm = calc_pair_pnl('LONG', 100, 50, 95, 55, 1.0, commission_pct=0.32)
        assert pnl_with_comm < pnl_no_comm
        assert abs(pnl_no_comm - pnl_with_comm - 0.32) < 0.01

    def test_zero_prices_returns_zero(self):
        pnl = calc_pair_pnl('LONG', 0, 50, 95, 55, 1.0)
        assert pnl == 0.0
