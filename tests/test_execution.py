# tests/test_execution.py
# Tsclp_v2.2_volatile — тесты ExecutionMonitor.
# Обновлено под register(**kwargs), check_all -> List, _extract_price(.., lot_size),
# resolve_uncertain, export_state/import_state, monitoring_error.

import pytest
from decimal import Decimal
from execution import ExecutionMonitor, PositionState, TrackedPosition
from tinkoff_client import PositionData


class FakeClient:
    def __init__(self):
        self.uncertain_state = False
        self.order_states = {}

    async def get_sandbox_order_state(self, order_id):
        return self.order_states.get(order_id)


def make_state(status_int, lots_executed, units, nano):
    class Q:
        pass

    class State:
        pass

    st = State()
    st.execution_report_status = status_int
    st.lots_executed = lots_executed
    q = Q()
    q.units = units
    q.nano = nano
    st.executed_order_price = q
    return st


def register_buy(mon, figi="FIGI", ticker="SBER", entry=100.0, qty=1, sl=99.0, tp=102.0, lot=1,
                 entry_requested=0):
    return mon.register(
        figi=figi, ticker=ticker, direction="BUY",
        entry_price=entry, quantity=qty, sl_price=sl, tp_price=tp,
        lot_size=lot, entry_requested_lots=entry_requested or qty,
    )


# --- Базовые SL/TP ---

@pytest.mark.asyncio
async def test_sl_trigger_buy():
    mon = ExecutionMonitor(FakeClient())
    register_buy(mon)
    to_close = await mon.check_all({"FIGI": 98.5})
    assert len(to_close) == 1
    pos = to_close[0]
    assert pos.close_reason == "SL"
    assert pos.state == PositionState.CLOSING
    assert pos.close_price_before_order == 98.5


@pytest.mark.asyncio
async def test_tp_trigger_buy():
    mon = ExecutionMonitor(FakeClient())
    register_buy(mon)
    to_close = await mon.check_all({"FIGI": 102.5})
    assert len(to_close) == 1
    assert to_close[0].close_reason == "TP"


@pytest.mark.asyncio
async def test_no_trigger_inside_range():
    mon = ExecutionMonitor(FakeClient())
    register_buy(mon)
    to_close = await mon.check_all({"FIGI": 100.5})
    assert len(to_close) == 0


@pytest.mark.asyncio
async def test_no_prices_sets_monitoring_error():
    client = FakeClient()
    mon = ExecutionMonitor(client)
    register_buy(mon)
    to_close = await mon.check_all({})
    assert mon.monitoring_error is True
    assert client.uncertain_state is False  # НЕ трогаем uncertain_state
    assert len(to_close) == 0


@pytest.mark.asyncio
async def test_missing_price_sets_monitoring_error():
    """Одна позиция с ценой, другая без — monitoring_error=True, состояние не меняется."""
    mon = ExecutionMonitor(FakeClient())
    register_buy(mon, figi="F1")
    register_buy(mon, figi="F2")
    to_close = await mon.check_all({"F1": 100.5})  # F2 без цены
    assert mon.monitoring_error is True
    # F2 остаётся OPEN
    assert mon.get_position("F2").state == PositionState.OPEN
    assert len(to_close) == 0


@pytest.mark.asyncio
async def test_double_trigger_only_once():
    mon = ExecutionMonitor(FakeClient())
    register_buy(mon)
    first = await mon.check_all({"FIGI": 98.0})
    second = await mon.check_all({"FIGI": 98.0})
    assert len(first) == 1
    assert len(second) == 0


# --- register(**kwargs), активные позиции ---

def test_register_kwargs():
    mon = ExecutionMonitor(FakeClient())
    pos = mon.register(
        figi="F", ticker="T", direction="BUY",
        entry_price=100.0, quantity=2, sl_price=99.0, tp_price=102.0,
        lot_size=10, trade_id="abc", signal_reason="test", signal_time="t0",
        price_before_order=100.1, order_send_time="t1",
        entry_requested_lots=2,
    )
    assert pos.figi == "F"
    assert pos.lot_size == 10
    assert pos.trade_id == "abc"
    assert pos.partial is False
    assert pos.logged is False
    assert pos.entry_requested_lots == 2
    assert mon.has_active_position("F") is True
    assert mon.active_count() == 1


def test_register_rejects_duplicate():
    """Активный дубликат по FIGI не перезаписывается."""
    mon = ExecutionMonitor(FakeClient())
    first = register_buy(mon, figi="F", qty=5)
    second = register_buy(mon, figi="F", qty=99)
    assert first is second  # вернулась существующая
    assert mon.get_position("F").quantity == 5


def test_has_active_position_false_for_unknown():
    mon = ExecutionMonitor(FakeClient())
    assert mon.has_active_position("NOPE") is False


def test_has_active_position_true_for_uncertain():
    mon = ExecutionMonitor(FakeClient())
    pos = register_buy(mon)
    pos.state = PositionState.UNCERTAIN
    assert mon.has_active_position("FIGI") is True
    assert mon.active_count() == 1


def test_has_active_position_false_for_closed():
    mon = ExecutionMonitor(FakeClient())
    pos = register_buy(mon)
    pos.state = PositionState.CLOSED
    assert mon.has_active_position("FIGI") is False
    assert mon.active_count() == 0


# --- resolve_closing: full fill ---

@pytest.mark.asyncio
async def test_resolve_closing_fill_full():
    client = FakeClient()
    mon = ExecutionMonitor(client)
    pos = register_buy(mon, qty=3)
    pos.state = PositionState.CLOSING
    pos.close_order_id = "ord-1"
    pos.close_requested_lots = 3
    client.order_states["ord-1"] = make_state(1, 3, 300, 0)
    await mon.resolve_closing(pos)
    assert pos.state == PositionState.CLOSED
    assert pos.executed_lots == 3
    # Цена за штуку: 300 / (3 * 1) = 100.0
    assert abs(pos.execution_price - 100.0) < 1e-9


# --- resolve_closing: status=1 при lots_executed < requested ---

@pytest.mark.asyncio
async def test_resolve_closing_fill_but_partial_becomes_uncertain():
    """Статус 1 (FILL), но lots_executed < close_requested_lots -> UNCERTAIN."""
    client = FakeClient()
    mon = ExecutionMonitor(client)
    pos = register_buy(mon, qty=5)
    pos.state = PositionState.CLOSING
    pos.close_order_id = "ord-partial-fill"
    pos.close_requested_lots = 5
    client.order_states["ord-partial-fill"] = make_state(1, 3, 300, 0)  # 3 < 5
    await mon.resolve_closing(pos)
    assert pos.state == PositionState.UNCERTAIN
    assert pos.quantity == 5  # не изменилось


# --- resolve_closing: status=5 PARTIALLYFILL ---

@pytest.mark.asyncio
async def test_resolve_closing_partial_fill_becomes_uncertain():
    client = FakeClient()
    mon = ExecutionMonitor(client)
    pos = register_buy(mon, qty=3)
    pos.state = PositionState.CLOSING
    pos.close_order_id = "ord-2"
    pos.close_requested_lots = 3
    client.order_states["ord-2"] = make_state(5, 2, 200, 0)
    await mon.resolve_closing(pos)
    assert pos.state == PositionState.UNCERTAIN
    assert pos.close_applied_lots == 2
    assert pos.quantity == 1  # 3 - 2


@pytest.mark.asyncio
async def test_resolve_closing_partial_fill_no_double_subtract():
    """Повторный вызов не вычитает уже учтённое исполнение."""
    client = FakeClient()
    mon = ExecutionMonitor(client)
    pos = register_buy(mon, qty=3)
    pos.state = PositionState.CLOSING
    pos.close_order_id = "ord-2b"
    pos.close_requested_lots = 3
    client.order_states["ord-2b"] = make_state(5, 2, 200, 0)
    await mon.resolve_closing(pos)
    assert pos.quantity == 1
    assert pos.close_applied_lots == 2
    # Повторный вызов (lots_executed не изменился)
    await mon.resolve_closing(pos)
    assert pos.quantity == 1  # не 0
    assert pos.close_applied_lots == 2


@pytest.mark.asyncio
async def test_resolve_closing_partial_fill_zero_lots_executed():
    """lots_executed=0 при статусе 5 не подменяет quantity."""
    client = FakeClient()
    mon = ExecutionMonitor(client)
    pos = register_buy(mon, qty=3)
    pos.state = PositionState.CLOSING
    pos.close_order_id = "ord-2c"
    pos.close_requested_lots = 3
    client.order_states["ord-2c"] = make_state(5, 0, 0, 0)
    await mon.resolve_closing(pos)
    assert pos.state == PositionState.UNCERTAIN
    assert pos.quantity == 3  # не изменилось


@pytest.mark.asyncio
async def test_resolve_closing_decreasing_lots_executed_is_uncertain():
    """
    Уменьшение lots_executed (противоречие) -> UNCERTAIN.
    quantity и close_applied_lots НЕ меняются (без самостоятельного исправления).
    """
    client = FakeClient()
    mon = ExecutionMonitor(client)
    pos = register_buy(mon, qty=5)
    pos.state = PositionState.CLOSING
    pos.close_order_id = "ord-dec"
    pos.close_requested_lots = 5
    pos.close_applied_lots = 3  # уже учтено 3 (искусственное состояние)
    client.order_states["ord-dec"] = make_state(5, 2, 200, 0)  # 2 < 3 — противоречие
    await mon.resolve_closing(pos)
    assert pos.state == PositionState.UNCERTAIN
    assert pos.quantity == 5  # НЕ меняется при противоречии
    assert pos.close_applied_lots == 3  # НЕ меняется


# --- resolve_closing: status 2/3 ---

@pytest.mark.asyncio
async def test_resolve_closing_rejected_returns_open():
    client = FakeClient()
    mon = ExecutionMonitor(client)
    pos = register_buy(mon)
    pos.state = PositionState.CLOSING
    pos.close_order_id = "ord-4"
    pos.close_requested_lots = 1
    client.order_states["ord-4"] = make_state(2, 0, 0, 0)
    await mon.resolve_closing(pos)
    assert pos.state == PositionState.OPEN
    assert pos.close_order_id is None


@pytest.mark.asyncio
async def test_resolve_closing_rejected_with_partial_becomes_uncertain():
    """Статус 2 (REJECTED) с lots_executed > 0 -> UNCERTAIN."""
    client = FakeClient()
    mon = ExecutionMonitor(client)
    pos = register_buy(mon, qty=3)
    pos.state = PositionState.CLOSING
    pos.close_order_id = "ord-rej-part"
    pos.close_requested_lots = 3
    client.order_states["ord-rej-part"] = make_state(2, 1, 100, 0)
    await mon.resolve_closing(pos)
    assert pos.state == PositionState.UNCERTAIN
    assert pos.close_applied_lots == 1
    assert pos.quantity == 2  # 3 - 1


@pytest.mark.asyncio
async def test_resolve_closing_cancelled_with_partial_becomes_uncertain():
    client = FakeClient()
    mon = ExecutionMonitor(client)
    pos = register_buy(mon, qty=5)
    pos.state = PositionState.CLOSING
    pos.close_order_id = "ord-canc-part"
    pos.close_requested_lots = 5
    client.order_states["ord-canc-part"] = make_state(3, 2, 200, 0)
    await mon.resolve_closing(pos)
    assert pos.state == PositionState.UNCERTAIN
    assert pos.close_applied_lots == 2
    assert pos.quantity == 3


# --- resolve_closing: state=None, no order_id ---

@pytest.mark.asyncio
async def test_resolve_closing_state_none_is_uncertain():
    client = FakeClient()
    mon = ExecutionMonitor(client)
    pos = register_buy(mon)
    pos.state = PositionState.CLOSING
    pos.close_order_id = "ord-3"
    await mon.resolve_closing(pos)
    assert pos.state == PositionState.UNCERTAIN


@pytest.mark.asyncio
async def test_resolve_closing_no_order_id_is_uncertain():
    mon = ExecutionMonitor(FakeClient())
    pos = register_buy(mon)
    pos.state = PositionState.CLOSING
    pos.close_order_id = None
    await mon.resolve_closing(pos)
    assert pos.state == PositionState.UNCERTAIN


# --- resolve_closing: idempotent ---

@pytest.mark.asyncio
async def test_resolve_closing_idempotent():
    client = FakeClient()
    mon = ExecutionMonitor(client)
    pos = register_buy(mon, qty=1)
    pos.state = PositionState.CLOSING
    pos.close_order_id = "ord-5"
    pos.close_requested_lots = 1
    client.order_states["ord-5"] = make_state(1, 1, 100, 0)

    await mon.resolve_closing(pos)
    assert pos.state == PositionState.CLOSED
    price_after_first = pos.execution_price

    await mon.resolve_closing(pos)
    assert pos.state == PositionState.CLOSED
    assert abs(pos.execution_price - price_after_first) < 1e-9
    assert mon.active_count() == 0


# --- _extract_price ---

def test_extract_price_divides_by_lots_and_lot_size():
    """Цена за штуку: total / (lots * lot_size)."""
    st = make_state(1, 3, 300, 0)
    assert abs(ExecutionMonitor._extract_price(st, 3, 1) - 100.0) < 1e-9


def test_extract_price_lot_size_gt_1():
    """lot_size > 1: total=10000, lots=10, lot=10 -> цена за штуку = 100.0."""
    st = make_state(1, 10, 10000, 0)
    assert abs(ExecutionMonitor._extract_price(st, 10, 10) - 100.0) < 1e-9


def test_extract_price_zero_lots_returns_none():
    st = make_state(1, 0, 0, 0)
    assert ExecutionMonitor._extract_price(st, 0, 1) is None


def test_extract_price_zero_lot_size_returns_none():
    st = make_state(1, 3, 300, 0)
    assert ExecutionMonitor._extract_price(st, 3, 0) is None


def test_extract_price_handles_none():
    class S:
        executed_order_price = None
    assert ExecutionMonitor._extract_price(S(), 1, 1) is None


# --- resolve_uncertain ---

def test_resolve_uncertain_no_broker_position_closes():
    mon = ExecutionMonitor(FakeClient())
    pos = register_buy(mon, qty=1)
    pos.state = PositionState.UNCERTAIN
    mon.resolve_uncertain(pos, {})  # FIGI нет у брокера
    assert pos.state == PositionState.CLOSED
    assert pos.quantity == 0


def test_resolve_uncertain_direction_mismatch_stays_uncertain():
    mon = ExecutionMonitor(FakeClient())
    pos = register_buy(mon, qty=1)  # direction=BUY (LONG)
    pos.state = PositionState.UNCERTAIN
    broker = {"FIGI": PositionData(lots=-1, direction="SHORT", raw_shares=Decimal(-1))}
    mon.resolve_uncertain(pos, broker)
    assert pos.state == PositionState.UNCERTAIN


def test_resolve_uncertain_quantity_mismatch_stays_uncertain():
    mon = ExecutionMonitor(FakeClient())
    pos = register_buy(mon, qty=1)
    pos.state = PositionState.UNCERTAIN
    broker = {"FIGI": PositionData(lots=2, direction="LONG", raw_shares=Decimal(2))}
    mon.resolve_uncertain(pos, broker)
    assert pos.state == PositionState.UNCERTAIN


def test_resolve_uncertain_unknown_direction_stays_uncertain():
    mon = ExecutionMonitor(FakeClient())
    pos = register_buy(mon, qty=1)
    pos.state = PositionState.UNCERTAIN
    broker = {"FIGI": PositionData(lots=1, direction="UNKNOWN", raw_shares=Decimal(1))}
    mon.resolve_uncertain(pos, broker)
    assert pos.state == PositionState.UNCERTAIN


def test_resolve_uncertain_match_opens():
    mon = ExecutionMonitor(FakeClient())
    pos = register_buy(mon, qty=1)
    pos.state = PositionState.UNCERTAIN
    broker = {"FIGI": PositionData(lots=1, direction="LONG", raw_shares=Decimal(1))}
    mon.resolve_uncertain(pos, broker)
    assert pos.state == PositionState.OPEN
    assert pos.quantity == 1


# --- export_state / import_state ---

def test_export_import_roundtrip():
    mon = ExecutionMonitor(FakeClient())
    register_buy(mon, figi="F1", qty=5)
    register_buy(mon, figi="F2", qty=3)
    mon.monitoring_error = True

    data = mon.export_state()
    assert "positions" in data
    assert data["monitoring_error"] is True

    mon2 = ExecutionMonitor(FakeClient())
    mon2.import_state(data)
    assert mon2.get_position("F1").quantity == 5
    assert mon2.get_position("F2").quantity == 3
    assert mon2.monitoring_error is True


def test_import_state_skips_bad_records():
    mon = ExecutionMonitor(FakeClient())
    data = {"positions": {"F1": {"figi": "F1", "ticker": "T", "direction": "BUY",
                                 "entry_price": 100.0, "quantity": 1,
                                 "sl_price": 99.0, "tp_price": 102.0,
                                 "state": "OPEN"},
                          "BAD": {"figi": "BAD"}}}  # неполные данные
    mon.import_state(data)
    assert mon.get_position("F1") is not None
    assert mon.get_position("BAD") is None