# execution.py
# Tsclp_v2.2_volatile — мониторинг позиций.
#
# ИЗМЕНЕНИЯ (этап 2):
#   - TrackedPosition: entry_requested_lots, entry_applied_lots,
#     close_requested_lots, close_applied_lots (раздельный учёт BUY/SELL).
#   - quantity — каноническое количество лотов (остаток).
#   - _extract_price(state, lots_executed, lot_size) — цена за ШТУКУ:
#     total / (lots_executed * lot_size). None при некорректных данных.
#   - resolve_closing(): убран `lots_executed or pos.quantity`;
#     CLOSED только при lots_executed >= close_requested_lots
#     И брокерском остатке 0 (сверка в main.py).
#     Накопленный учёт: new = lots_executed - close_applied_lots;
#     при new < 0 или противоречии — UNCERTAIN, без самостоятельного
#     исправления quantity.
#     Статусы 2/3 с lots_executed > 0 -> UNCERTAIN (сверка в main.py).
#   - resolve_uncertain(pos, broker_positions) — новый (def, не async).
#     Сверка с брокерским портфелем (dict[str, PositionData]).
#     Учёт направления и количества. Неизвестное направление или
#     несоответствие количества -> UNCERTAIN.
#   - check_all(): monitoring_error (флаг ExecutionMonitor),
#     не client.uncertain_state. Сбрасывается в начале каждого вызова;
#     ставится True, если хотя бы у одной активной позиции нет цены.
#     Состояние позиции НЕ меняется.
#   - register(): защита от активного дубликата FIGI; валидация.
#   - export_state() / import_state(data) — для persistence (JSON-совместимый).
#   - monitoring_error — новый флаг.
#
# НЕ ИЗМЕНЕНО: PositionState, get_position, has_active_position,
#   active_count, имена остальных полей TrackedPosition.

import logging
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Dict, List, Optional

import config


class PositionState(str, Enum):
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    UNCERTAIN = "UNCERTAIN"


@dataclass
class TrackedPosition:
    figi: str
    ticker: str
    direction: str
    entry_price: float
    quantity: int                       # каноническое количество лотов (остаток)
    sl_price: float
    tp_price: float
    lot_size: int = 1
    state: PositionState = PositionState.OPEN
    close_reason: Optional[str] = None
    close_order_id: Optional[str] = None
    close_requested_at: Optional[str] = None
    close_price_before_order: float = 0.0
    executed_lots: int = 0
    execution_price: Optional[float] = None
    trade_id: str = ""
    signal_reason: str = ""
    signal_time: str = ""
    price_before_order: float = 0.0
    order_send_time: str = ""
    partial: bool = False
    logged: bool = False
    actual_commission: Optional[float] = None
    entry_actual_commission: Optional[float] = None
    entry_order_id: Optional[str] = None
    # Раздельный учёт BUY
    entry_requested_lots: int = 0
    entry_applied_lots: int = 0
    # Раздельный учёт SELL
    close_requested_lots: int = 0
    close_applied_lots: int = 0

    def to_dict(self) -> dict:
        """Сериализация для persistence. JSON-совместимый dict."""
        d = asdict(self)
        d["state"] = self.state.value  # Enum -> str
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "TrackedPosition":
        """Десериализация. Неизвестные ключи игнорируются."""
        d = dict(data)
        if "state" in d and isinstance(d["state"], str):
            d["state"] = PositionState(d["state"])
        # Убираем ключи, которых нет в dataclass (защита от старых форматов)
        allowed = {f.name for f in cls.__dataclass_fields__.values()}
        d = {k: v for k, v in d.items() if k in allowed}
        return cls(**d)


class ExecutionMonitor:
    def __init__(self, client):
        self.client = client
        self.positions: Dict[str, TrackedPosition] = {}
        self.monitoring_error: bool = False

    def register(self, **kwargs) -> TrackedPosition:
        """
        Регистрирует позицию.
        Защита от активного дубликата по FIGI: если уже есть активная
        позиция (OPEN/CLOSING/UNCERTAIN) — не перезаписываем, логируем
        ошибку, возвращаем существующую.
        Валидация: quantity > 0, lot_size > 0, entry_price > 0, sl/tp > 0.
        """
        figi = kwargs.get("figi")
        if figi and self.has_active_position(figi):
            existing = self.positions[figi]
            logging.error(
                f"[execution] register: активная позиция по {figi} уже есть "
                f"(state={existing.state.value}). Перезапись запрещена."
            )
            return existing

        # Валидация (мягкая: не бросаем исключение, логируем и возвращаем как есть,
        # чтобы main.py мог решить; но для критических полей — проверяем).
        qty = kwargs.get("quantity")
        lot = kwargs.get("lot_size", 1)
        entry = kwargs.get("entry_price")
        sl = kwargs.get("sl_price")
        tp = kwargs.get("tp_price")
        if qty is not None and qty <= 0:
            logging.error(f"[execution] register: quantity={qty} <= 0")
        if lot is not None and lot <= 0:
            logging.error(f"[execution] register: lot_size={lot} <= 0")
        if entry is not None and entry <= 0:
            logging.error(f"[execution] register: entry_price={entry} <= 0")
        if sl is not None and sl <= 0:
            logging.error(f"[execution] register: sl_price={sl} <= 0")
        if tp is not None and tp <= 0:
            logging.error(f"[execution] register: tp_price={tp} <= 0")

        pos = TrackedPosition(**kwargs)
        self.positions[pos.figi] = pos
        logging.info(
            f"[execution] OPEN {pos.ticker} {pos.direction} "
            f"entry={pos.entry_price:.4f} sl={pos.sl_price:.4f} tp={pos.tp_price:.4f} "
            f"qty={pos.quantity} lot={pos.lot_size} partial={pos.partial} "
            f"entry_req={pos.entry_requested_lots} entry_applied={pos.entry_applied_lots}"
        )
        return pos

    def get_position(self, figi: str) -> Optional[TrackedPosition]:
        return self.positions.get(figi)

    def has_active_position(self, figi: str) -> bool:
        p = self.positions.get(figi)
        return p is not None and p.state in (PositionState.OPEN, PositionState.CLOSING, PositionState.UNCERTAIN)

    async def check_all(self, prices: Dict[str, float]) -> List[TrackedPosition]:
        """
        Проверяет SL/TP для OPEN-позиций.
        monitoring_error сбрасывается в начале, ставится True, если хотя бы
        у одной активной позиции нет цены. Состояние позиции НЕ меняется.
        Возвращает список позиций, которые нужно закрыть (SL/TP).
        """
        to_close: List[TrackedPosition] = []
        self.monitoring_error = False

        if prices is None:
            logging.error("[execution] prices=None — monitoring_error=True")
            self.monitoring_error = True
            return to_close

        if not prices:
            logging.warning("[execution] prices={} — monitoring_error=True")
            self.monitoring_error = True
            return to_close

        for pos in list(self.positions.values()):
            if pos.state != PositionState.OPEN:
                continue
            price = prices.get(pos.figi)
            if price is None or price <= 0:
                logging.warning(
                    f"[execution] {pos.ticker}: нет цены — monitoring_error=True, "
                    f"позиция без контроля (state={pos.state.value})"
                )
                self.monitoring_error = True
                continue
            if pos.direction == "BUY":
                if price <= pos.sl_price:
                    pos.state = PositionState.CLOSING
                    pos.close_reason = "SL"
                    pos.close_price_before_order = price
                    to_close.append(pos)
                elif price >= pos.tp_price:
                    pos.state = PositionState.CLOSING
                    pos.close_reason = "TP"
                    pos.close_price_before_order = price
                    to_close.append(pos)
        return to_close

    async def resolve_closing(self, pos: TrackedPosition):
        """
        Разрешает CLOSING-позицию по статусу заявки.
        Правила:
          - нет order_id -> UNCERTAIN;
          - state=None -> UNCERTAIN;
          - статус 1 (FILL): CLOSED только при lots_executed >= close_requested_lots.
            Иначе — частичное исполнение, накопленный учёт, UNCERTAIN (сверка в main.py).
          - статус 5 (PARTIALLYFILL): накопленный учёт, UNCERTAIN.
          - статусы 2/3 (REJECTED/CANCELLED): если lots_executed > 0 -> UNCERTAIN
            (сверка в main.py); иначе OPEN, close_order_id=None.
          - иначе -> UNCERTAIN.
        Накопленный учёт: new = lots_executed - close_applied_lots.
          Если new < 0 (уменьшение) — UNCERTAIN, без исправления quantity.
          Если new >= 0: close_applied_lots = lots_executed;
          quantity -= new (только для SELL-закрытия лонга).
        """
        if not pos.close_order_id:
            pos.state = PositionState.UNCERTAIN
            logging.error(f"[execution] {pos.ticker}: CLOSING без order_id → UNCERTAIN")
            return

        try:
            state = await self.client.get_sandbox_order_state(pos.close_order_id)
            if state is None:
                pos.state = PositionState.UNCERTAIN
                logging.error(f"[execution] {pos.ticker}: state=None → UNCERTAIN")
                return

            raw_status = getattr(state, "execution_report_status", None)
            try:
                status_int = int(raw_status) if raw_status is not None else -1
            except (TypeError, ValueError):
                status_int = -1

            lots_executed = int(getattr(state, "lots_executed", 0) or 0)
            actual_commission = self._extract_commission(state)

            # close_requested_lots: если не задано — используем quantity (для совместимости)
            requested = pos.close_requested_lots or pos.quantity

            logging.info(
                f"[resolve] {pos.ticker} order={pos.close_order_id} "
                f"raw_status={raw_status!r} status_int={status_int} "
                f"lots_executed={lots_executed} requested={requested} "
                f"qty={pos.quantity} close_applied={pos.close_applied_lots} "
                f"actual_commission={actual_commission}"
            )

            if status_int == 1:
                # FILL: CLOSED только при полном исполнении
                if lots_executed >= requested and requested > 0:
                    new = lots_executed - pos.close_applied_lots
                    if new < 0:
                        logging.error(
                            f"[execution] {pos.ticker}: lots_executed={lots_executed} "
                            f"< close_applied={pos.close_applied_lots} — противоречие, UNCERTAIN"
                        )
                        pos.state = PositionState.UNCERTAIN
                        return
                    # Накопленный учёт
                    pos.close_applied_lots = lots_executed
                    if new > 0 and pos.direction == "BUY":
                        pos.quantity = max(0, pos.quantity - new)
                    pos.executed_lots = lots_executed
                    pos.execution_price = self._extract_price(state, lots_executed, pos.lot_size)
                    pos.actual_commission = actual_commission
                    pos.state = PositionState.CLOSED
                    logging.info(f"[execution] CLOSED {pos.ticker} @ {pos.execution_price}")
                else:
                    # Частичное исполнение при статусе 1 (аномалия) — UNCERTAIN
                    logging.error(
                        f"[execution] {pos.ticker}: status=1, но lots_executed={lots_executed} "
                        f"< requested={requested} — UNCERTAIN"
                    )
                    pos.state = PositionState.UNCERTAIN

            elif status_int == 5:
                # PARTIALLYFILL: накопленный учёт, UNCERTAIN
                new = lots_executed - pos.close_applied_lots
                if new < 0:
                    logging.error(
                        f"[execution] {pos.ticker}: lots_executed={lots_executed} "
                        f"< close_applied={pos.close_applied_lots} — противоречие, UNCERTAIN"
                    )
                    pos.state = PositionState.UNCERTAIN
                    return
                pos.close_applied_lots = lots_executed
                if new > 0 and pos.direction == "BUY":
                    pos.quantity = max(0, pos.quantity - new)
                pos.executed_lots = lots_executed
                pos.actual_commission = actual_commission
                pos.state = PositionState.UNCERTAIN
                logging.warning(
                    f"[execution] {pos.ticker}: PARTIALLYFILL {lots_executed}/{requested} "
                    f"(учтено new={new}, qty={pos.quantity}) → UNCERTAIN"
                )

            elif status_int in (2, 3):
                if lots_executed > 0:
                    # Отмена/отклонение после частичного исполнения — UNCERTAIN
                    new = lots_executed - pos.close_applied_lots
                    if new >= 0:
                        pos.close_applied_lots = lots_executed
                        if new > 0 and pos.direction == "BUY":
                            pos.quantity = max(0, pos.quantity - new)
                    pos.state = PositionState.UNCERTAIN
                    logging.warning(
                        f"[execution] {pos.ticker}: status={status_int} с lots_executed={lots_executed} "
                        f"→ UNCERTAIN (сверка в main.py)"
                    )
                else:
                    pos.state = PositionState.OPEN
                    pos.close_order_id = None
                    logging.warning(
                        f"[execution] {pos.ticker}: закрытие отклонено/отменено (lots_executed=0) — снова OPEN"
                    )
            else:
                pos.state = PositionState.UNCERTAIN
                logging.warning(f"[execution] {pos.ticker}: статус {status_int} → UNCERTAIN")

        except Exception as e:
            logging.error(f"[execution] {pos.ticker}: ошибка проверки статуса: {type(e).__name__}: {e}")
            pos.state = PositionState.UNCERTAIN

    def resolve_uncertain(self, pos: TrackedPosition, broker_positions: Dict[str, "PositionData"]):
        """
        Разрешает UNCERTAIN-позицию сверкой с брокерским портфелем.
        broker_positions: dict[figi, PositionData] (из tinkoff_client.get_positions).
        Правила:
          - FIGI отсутствует у брокера: если портфель достоверный — CLOSED (quantity=0);
            но достоверность обеспечивает вызывающий код (main.py). Если портфель
            неполный — main.py не должен вызывать resolve_uncertain.
          - FIGI есть у брокера: сверить lots (лоты со знаком) с pos.quantity.
            Если lots != pos.quantity (с учётом направления) — UNCERTAIN.
            Если lots == pos.quantity — обновить quantity, state=OPEN.
          - Направление: если direction у брокера SHORT, а у нас BUY (LONG) — UNCERTAIN.
            Если direction == UNKNOWN — UNCERTAIN.
        Не async: портфель уже загружен вызывающим кодом.
        """
        figi = pos.figi
        broker = broker_positions.get(figi)

        if broker is None:
            # FIGI нет у брокера. Если портфель достоверный — считаем CLOSED.
            # Достоверность обеспечивает main.py (не вызывает при None/неполном).
            pos.quantity = 0
            pos.state = PositionState.CLOSED
            logging.info(
                f"[execution] resolve_uncertain {pos.ticker}: нет у брокера → CLOSED"
            )
            return

        if broker.direction == "UNKNOWN":
            logging.error(
                f"[execution] resolve_uncertain {pos.ticker}: direction=UNKNOWN → остаётся UNCERTAIN"
            )
            pos.state = PositionState.UNCERTAIN
            return

        # Проверка направления: у нас direction="BUY" (LONG), у брокера SHORT — расхождение
        our_side = "LONG" if pos.direction == "BUY" else "SHORT"
        if broker.direction != our_side:
            logging.error(
                f"[execution] resolve_uncertain {pos.ticker}: направление брокера={broker.direction}, "
                f"локально={our_side} — расхождение, остаётся UNCERTAIN"
            )
            pos.state = PositionState.UNCERTAIN
            return

        # Сверка количества (лоты)
        if broker.lots != pos.quantity:
            logging.error(
                f"[execution] resolve_uncertain {pos.ticker}: lots брокера={broker.lots}, "
                f"локально={pos.quantity} — расхождение, остаётся UNCERTAIN"
            )
            pos.state = PositionState.UNCERTAIN
            return

        # Всё сходится
        pos.quantity = broker.lots
        pos.state = PositionState.OPEN
        logging.info(
            f"[execution] resolve_uncertain {pos.ticker}: сверка OK (lots={broker.lots}) → OPEN"
        )

    @staticmethod
    def _extract_price(state, lots_executed: int, lot_size: int) -> Optional[float]:
        """
        Возвращает цену за ОДНУ БУМАГУ (штуку).
        executed_order_price = суммарная стоимость исполнения.
        price_per_share = total / (lots_executed * lot_size).
        None при некорректных данных (lots_executed <= 0, lot_size <= 0, нет цены).
        """
        try:
            ep = getattr(state, "executed_order_price", None)
            if ep is None:
                return None
            units = getattr(ep, "units", 0)
            nano = getattr(ep, "nano", 0)
            total = float(units) + float(nano) / 1e9
            if lots_executed > 0 and lot_size > 0:
                return total / (lots_executed * lot_size)
            return None
        except Exception:
            return None

    @staticmethod
    def _extract_commission(state) -> Optional[float]:
        try:
            ec = getattr(state, "executed_commission", None)
            if ec is None:
                return None
            units = getattr(ec, "units", 0)
            nano = getattr(ec, "nano", 0)
            value = abs(float(units) + float(nano) / 1e9)
            if value <= 0:
                return None
            return value
        except Exception:
            return None

    def active_count(self) -> int:
        return sum(
            1 for p in self.positions.values()
            if p.state in (PositionState.OPEN, PositionState.CLOSING, PositionState.UNCERTAIN)
        )

    def export_state(self) -> dict:
        """Сериализация всех позиций для persistence (JSON-совместимый dict)."""
        return {
            "positions": {figi: pos.to_dict() for figi, pos in self.positions.items()},
            "monitoring_error": self.monitoring_error,
        }

    def import_state(self, data: dict) -> None:
        """
        Восстановление позиций из persistence.
        Неизвестные ключи игнорируются. Некорректные записи пропускаются с логом.
        """
        if not isinstance(data, dict):
            logging.error("[execution] import_state: data не dict")
            return
        self.positions = {}
        positions = data.get("positions", {})
        if not isinstance(positions, dict):
            logging.error("[execution] import_state: positions не dict")
            return
        for figi, pos_data in positions.items():
            try:
                self.positions[figi] = TrackedPosition.from_dict(pos_data)
            except Exception as e:
                logging.error(
                    f"[execution] import_state: не удалось восстановить {figi}: "
                    f"{type(e).__name__}: {e}"
                )
        self.monitoring_error = bool(data.get("monitoring_error", False))