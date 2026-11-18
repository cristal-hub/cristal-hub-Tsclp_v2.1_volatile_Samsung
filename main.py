# main.py
# Tsclp_v2.2_volatile — основной цикл (ТОЛЬКО ПЕСОЧНИЦА).
#
# ИЗМЕНЕНИЯ (этап 4):
#   - Цены: TOP-10 + открытые позиции (объединение FIGI).
#   - prices=None -> monitoring_error, мониторинг продолжается.
#   - reconcile: двусторонний, с количествами и знаком (PositionData).
#   - process_closing: обработка UNCERTAIN через resolve_uncertain.
#   - partial BUY: cancel_order + подтверждённый финальный статус.
#   - потерянный ответ BUY: OrderResult.order_id сохраняется.
#   - _extract_price: цена за штуку (lots * lot_size).
#   - persistence (state.json): атомарная запись, обязательный reconcile.
#   - monitoring_error проверяется перед входами.
#   - trading.flag: main() НЕ создаёт автоматически.
#
# ИЗМЕНЕНИЯ (этап 4.1):
#   - trading_cycle: при status != FILL (например, NEW) — НЕ просто continue,
#     а запрос get_sandbox_order_state(order_id) и проверка lots_executed.
#     Если lots_executed > 0 -> создать TrackedPosition (как при FILL),
#     сохранить state.json, отправить POSITION_OPENED / notify.
#     Логирование ORDER_NOT_FILLED в JSONL (раньше — только logging.warning
#     в stdout, что терялось).
#
# НЕ ИЗМЕНЕНО: стратегия, calculate_sl_tp, _log_strategy_diagnostic,
#   notify, Telegram-lifecycle, trading.flag логика, reconcile,
#   process_closing, _save_state/_load_state.

import asyncio
import csv
import json
import logging
import os
import uuid
from datetime import datetime, timezone

import pandas as pd
from t_tech.invest import (
    CandleInterval,
    OrderDirection,
    OrderExecutionReportStatus,
)

import config
import logger as research_logger
from risk_manager import RiskManager
from strategy import StrategyManager
from tinkoff_client import TinkoffClient, PositionData
from execution import ExecutionMonitor, PositionState
from screener import Screener

if config.ENABLE_TELEGRAM:
    from telegram_bot import TelegramBot
else:
    TelegramBot = None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telegram.ext._utils.networkloop").setLevel(logging.WARNING)

TRADING_FLAG = "trading.flag"
STATE_FILE = "state.json"
STATE_FILE_TMP = "state.json.tmp"

top_instruments = []
telegram_bot = None
main_client = None
reconcile_done = False
monitor_ref = None
rm_ref = None

_strategy_event_keys = set()


def is_trading_enabled() -> bool:
    return os.path.exists(TRADING_FLAG)


def start_trading():
    try:
        with open(TRADING_FLAG, "w", encoding="utf-8") as f:
            f.write(datetime.now(timezone.utc).isoformat())
        logging.info("✅ Торговля ВКЛЮЧЕНА (создан trading.flag)")
        research_logger.log_event("TRADING_START", {})
    except Exception as e:
        logging.error(f"Не удалось создать {TRADING_FLAG}: {type(e).__name__}: {e}")


def stop_trading():
    try:
        if os.path.exists(TRADING_FLAG):
            os.remove(TRADING_FLAG)
        logging.info("🛑 Торговля ВЫКЛЮЧЕНА (удалён trading.flag)")
        research_logger.log_event("TRADING_STOP", {})
    except Exception as e:
        logging.error(f"Не удалось удалить {TRADING_FLAG}: {type(e).__name__}: {e}")


def get_status():
    instruments = ", ".join(i['ticker'] for i in top_instruments) or "—"
    monitored = ", ".join(
        p.ticker for p in (monitor_ref.positions.values() if monitor_ref else [])
        if p.state in (PositionState.OPEN, PositionState.CLOSING, PositionState.UNCERTAIN)
    ) or "—"
    uncertain = "ДА" if (main_client and main_client.is_uncertain()) else "нет"
    trading = "АКТИВНА" if is_trading_enabled() else "СТОП"
    tg_status = "работает" if telegram_bot else "НЕДОСТУПЕН"
    monitoring_err = "ДА" if (monitor_ref and monitor_ref.monitoring_error) else "нет"
    return (
        f"🤖 Статус\n"
        f"Торговля: {trading}\n"
        f"Telegram: {tg_status}\n"
        f"Режим: SANDBOX\n"
        f"Сверка: {'OK' if reconcile_done else 'НЕ ЗАВЕРШЕНА'}\n"
        f"Неопределённость: {uncertain}\n"
        f"Ошибка мониторинга цен: {monitoring_err}\n"
        f"Для новых входов ({len(top_instruments)}): {instruments}\n"
        f"Под мониторингом ({monitor_ref.active_count() if monitor_ref else 0}): {monitored}"
    )


async def notify(text: str):
    logging.info(f"[notify] {text}")
    if telegram_bot:
        try:
            await telegram_bot.send_message(text)
        except Exception as e:
            logging.warning(f"Telegram send error: {type(e).__name__}: {e}")


async def _stop_telegram_bot():
    global telegram_bot
    if telegram_bot is None:
        return
    try:
        try:
            await asyncio.wait_for(telegram_bot.application.updater.stop(), timeout=10.0)
        except Exception as e:
            logging.warning(f"[tg] updater.stop(): {type(e).__name__}: {e}")
        try:
            await asyncio.wait_for(telegram_bot.application.stop(), timeout=10.0)
        except Exception as e:
            logging.warning(f"[tg] application.stop(): {type(e).__name__}: {e}")
        try:
            await asyncio.wait_for(telegram_bot.application.shutdown(), timeout=10.0)
        except Exception as e:
            logging.warning(f"[tg] application.shutdown(): {type(e).__name__}: {e}")
        logging.info("[tg] Telegram-бот остановлен.")
    except Exception as e:
        logging.warning(f"[tg] Ошибка остановки: {type(e).__name__}: {e}")
    finally:
        telegram_bot = None


async def _try_create_telegram_bot() -> bool:
    global telegram_bot
    if not (config.ENABLE_TELEGRAM and TelegramBot):
        return False
    try:
        bot = TelegramBot(
            config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID,
            config.TELEGRAM_ALLOWED_USER_ID,
            {'start_trading': start_trading, 'stop_trading': stop_trading, 'get_status': get_status},
        )
        await asyncio.wait_for(bot.application.initialize(), timeout=20.0)
        await asyncio.wait_for(bot.application.start(), timeout=20.0)
        await asyncio.wait_for(bot.application.updater.start_polling(), timeout=20.0)
        await bot.register_commands()
        telegram_bot = bot
        logging.info("[tg] Telegram-бот создан и запущен (initialize → start → polling).")
        await notify("🤖 Tsclp_v2.2_volatile: Telegram восстановлен.")
        return True
    except Exception as e:
        logging.warning(f"[tg] Не удалось создать Telegram-бот: {type(e).__name__}: {e!r}")
        await _stop_telegram_bot()
        return False


async def _telegram_watchdog():
    if not (config.ENABLE_TELEGRAM and TelegramBot):
        return
    failures = 0
    while True:
        await asyncio.sleep(60)
        if telegram_bot is None:
            await _try_create_telegram_bot()
            continue
        try:
            await asyncio.wait_for(telegram_bot.application.bot.get_me(), timeout=15.0)
            if failures > 0:
                logging.info(f"[tg-watchdog] Telegram восстановлен после {failures} неудач.")
            failures = 0
        except Exception as e:
            failures += 1
            logging.warning(f"[tg-watchdog] Telegram недоступен ({failures}/3): {type(e).__name__}: {e!r}")
            if failures >= 3:
                logging.info("[tg-watchdog] Полный перезапуск Telegram...")
                await _stop_telegram_bot()
                failures = 0


def _trade_id_already_logged(trade_id: str) -> bool:
    if not trade_id:
        return False
    try:
        if not os.path.isfile(research_logger.TRADES_FILE):
            return False
        with open(research_logger.TRADES_FILE, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                if row.get("trade_id") == trade_id:
                    return True
        return False
    except Exception as e:
        logging.warning(f"[csv] Не удалось проверить trade_id {trade_id}: {type(e).__name__}: {e}")
        return True


def _timestamp_for_log(value):
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def _log_strategy_diagnostic(
    event_type: str,
    ticker: str,
    figi: str,
    signal: str,
    reason: str,
    sm: StrategyManager,
    df: pd.DataFrame,
    price: float,
):
    try:
        candle_time = getattr(sm, "last_signal_candle_time", None)
        if candle_time is None and df is not None and not df.empty:
            candle_time = df.index[-1]
        candle_time_text = _timestamp_for_log(candle_time)
        if candle_time_text is None:
            candle_time_text = "UNKNOWN"
        event_key = (event_type, ticker, candle_time_text)
        if event_key in _strategy_event_keys:
            return

        last_close = None
        last_volume = None
        if df is not None and not df.empty:
            if "close" in df.columns:
                value = df["close"].iloc[-1]
                if pd.notna(value):
                    last_close = float(value)
            if "volume" in df.columns:
                value = df["volume"].iloc[-1]
                if pd.notna(value):
                    last_volume = float(value)

        adx_value = getattr(sm, "last_adx_value", None)
        if adx_value is not None:
            try:
                adx_value = float(adx_value)
            except (TypeError, ValueError):
                adx_value = None

        payload = {
            "ticker": ticker, "figi": figi, "signal": signal, "reason": reason,
            "signal_candle_time": candle_time_text,
            "candles_count": int(len(df)) if df is not None else 0,
            "regime": getattr(sm, "_last_regime", None),
            "regime_candidate": getattr(sm, "_last_regime_candidate", None),
            "regime_streak": getattr(sm, "_regime_streak", None),
            "adx": adx_value, "price": float(price),
            "last_close": last_close, "last_volume": last_volume,
        }
        if research_logger.log_event(event_type, payload):
            _strategy_event_keys.add(event_key)
    except Exception as e:
        logging.warning(
            f"[strategy-log] Не удалось записать {event_type} "
            f"для {ticker}: {type(e).__name__}: {e}"
        )


# ============================================================
# PERSISTENCE (state.json)
# ============================================================

def _save_state(monitor: ExecutionMonitor, rm: RiskManager) -> bool:
    try:
        data = {
            "positions": monitor.export_state().get("positions", {}),
            "monitoring_error": monitor.monitoring_error,
            "risk": rm.export_state(),
        }
        with open(STATE_FILE_TMP, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(STATE_FILE_TMP, STATE_FILE)
        return True
    except Exception as e:
        logging.error(f"[state] Ошибка сохранения: {type(e).__name__}: {e}")
        return False


def _load_state(monitor: ExecutionMonitor, rm: RiskManager) -> bool:
    if not os.path.isfile(STATE_FILE):
        logging.warning(f"[state] {STATE_FILE} отсутствует — новые входы запрещены до сверки.")
        return False
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            logging.error("[state] Корневой объект не dict")
            return False
        positions = data.get("positions", {})
        if not isinstance(positions, dict):
            logging.error("[state] positions не dict")
            return False
        monitor.import_state({
            "positions": positions,
            "monitoring_error": bool(data.get("monitoring_error", False)),
        })
        risk = data.get("risk", {})
        if isinstance(risk, dict):
            rm.import_state(risk)
        logging.info(f"[state] Загружено позиций: {len(monitor.positions)}")
        return True
    except Exception as e:
        logging.error(f"[state] Ошибка загрузки: {type(e).__name__}: {e}")
        return False


# ============================================================
# RECONCILE (двусторонний)
# ============================================================

async def reconcile(client: TinkoffClient, monitor: ExecutionMonitor):
    global reconcile_done
    positions = await client.get_positions()

    if positions is None:
        logging.error("[reconcile] Ошибка получения портфеля — состояние неизвестно, сверка не завершена.")
        client.uncertain_state = True
        reconcile_done = False
        return

    broker = {f: p for f, p in positions.items()
              if not f.startswith(("RUB", "USD", "EUR"))}

    local = {f: p for f, p in monitor.positions.items()
             if p.state in (PositionState.OPEN, PositionState.CLOSING, PositionState.UNCERTAIN)}

    mismatches = []

    for figi, pd in broker.items():
        pos = local.get(figi)
        if pos is None:
            mismatches.append(f"{figi}: есть у брокера ({pd.lots}), нет локально")
            continue
        our_side = "LONG" if pos.direction == "BUY" else "SHORT"
        if pd.direction != our_side:
            mismatches.append(f"{figi}: направление брокера={pd.direction}, локально={our_side}")
            continue
        if pd.lots != pos.quantity:
            mismatches.append(f"{figi}: lots брокера={pd.lots}, локально={pos.quantity}")

    for figi, pos in local.items():
        if figi not in broker:
            mismatches.append(f"{figi}: есть локально ({pos.quantity}), нет у брокера")

    if mismatches:
        client.uncertain_state = True
        reconcile_done = False
        msg = "⚠️ Расхождения: " + "; ".join(mismatches)
        logging.error(f"[reconcile] {msg}")
        research_logger.log_event("RECONCILE_MISMATCH", {"mismatches": mismatches})
        await notify(msg)
        return

    if not local and not broker:
        logging.info("[reconcile] Позиций нет (достоверно).")
    else:
        logging.info(f"[reconcile] Все позиции совпадают ({len(local)} активных).")

    reconcile_done = True
    client.uncertain_state = False
    research_logger.log_event("RECONCILE_OK", {"positions": len(local)})


# ============================================================
# CLOSE / PROCESS CLOSING
# ============================================================

async def close_position(client: TinkoffClient, pos, price: float):
    direction = (
        OrderDirection.ORDER_DIRECTION_SELL if pos.direction == "BUY"
        else OrderDirection.ORDER_DIRECTION_BUY
    )
    result = await client.post_market_order(pos.figi, pos.quantity, direction)

    if result.state is None:
        pos.state = PositionState.UNCERTAIN
        client.uncertain_state = True
        logging.error(
            f"[close] {pos.ticker}: state=None (order_id={result.order_id}) — UNCERTAIN. "
            f"Повторная отправка запрещена."
        )
        research_logger.log_event("CLOSE_FAILED", {
            "trade_id": pos.trade_id, "ticker": pos.ticker, "figi": pos.figi,
            "order_id": result.order_id,
        })
        return

    pos.close_order_id = result.order_id
    pos.close_requested_lots = pos.quantity
    pos.close_requested_at = datetime.now(timezone.utc).isoformat()
    logging.info(
        f"🔒 [CLOSE] {pos.ticker} {pos.close_reason} @ {price:.4f} → CLOSING "
        f"(id={result.order_id}, requested={pos.close_requested_lots})"
    )
    research_logger.log_event("CLOSE_SENT", {
        "trade_id": pos.trade_id,
        "ticker": pos.ticker, "figi": pos.figi, "reason": pos.close_reason,
        "order_id": result.order_id, "price_before_order": price,
        "requested_lots": pos.close_requested_lots,
    })


async def process_closing(client: TinkoffClient, monitor: ExecutionMonitor):
    global reconcile_done

    for pos in list(monitor.positions.values()):
        if pos.state == PositionState.CLOSING:
            await monitor.resolve_closing(pos)

    uncertain_positions = [p for p in monitor.positions.values() if p.state == PositionState.UNCERTAIN]
    if uncertain_positions:
        positions = await client.get_positions()
        if positions is None:
            logging.error("[process_closing] get_positions()=None — UNCERTAIN не разрешаются.")
            client.uncertain_state = True
            reconcile_done = False
        else:
            broker = {f: p for f, p in positions.items()
                      if not f.startswith(("RUB", "USD", "EUR"))}
            for pos in uncertain_positions:
                monitor.resolve_uncertain(pos, broker)
            await reconcile(client, monitor)

    for pos in list(monitor.positions.values()):
        if pos.state != PositionState.CLOSED or pos.logged:
            continue

        trade_id = pos.trade_id or uuid.uuid4().hex[:12]
        if _trade_id_already_logged(trade_id):
            logging.warning(f"[csv] {pos.ticker}: trade_id={trade_id} уже в CSV — помечаю logged=True")
            pos.logged = True
            continue

        exit_price = pos.execution_price or 0.0
        executed_lots = pos.executed_lots
        if executed_lots <= 0:
            logging.error(
                f"[csv] {pos.ticker}: executed_lots={executed_lots} — пропуск записи"
            )
            pos.logged = True
            continue
        shares = executed_lots * pos.lot_size

        entry_slippage = (pos.entry_price - pos.price_before_order) * shares
        exit_slippage = (pos.close_price_before_order - exit_price) * shares
        total_slippage = entry_slippage + exit_slippage

        gross_pnl = (exit_price - pos.entry_price) * shares
        turnover = (exit_price + pos.entry_price) * shares
        model_total_commission = abs(turnover * config.COMMISSION_PER_SIDE)

        if pos.actual_commission is not None:
            exit_commission = pos.actual_commission
        else:
            exit_commission = model_total_commission / 2.0

        if pos.entry_actual_commission is not None:
            entry_commission = pos.entry_actual_commission
        else:
            entry_commission = model_total_commission / 2.0

        total_commission = entry_commission + exit_commission

        entry_actual = pos.entry_actual_commission is not None
        exit_actual = pos.actual_commission is not None
        if entry_actual and exit_actual:
            commission_source = "actual"
        elif entry_actual or exit_actual:
            commission_source = "mixed"
        else:
            commission_source = "model"

        net_pnl = gross_pnl - total_commission
        slippage_percent = (total_slippage / turnover * 100.0) if turnover > 0 else 0.0

        cumulative_before = research_logger.get_cumulative_net_pnl()
        cumulative_after = cumulative_before + net_pnl

        trade_row = {
            "trade_id": trade_id,
            "timestamp_utc": pos.close_requested_at or "",
            "ticker": pos.ticker, "figi": pos.figi, "lot_size": pos.lot_size,
            "side": pos.direction,
            "signal_reason": pos.signal_reason,
            "signal_time": pos.signal_time,
            "price_before_order": pos.price_before_order,
            "order_send_time": pos.order_send_time,
            "order_id": pos.close_order_id or "",
            "execution_status": "FILL_PARTIAL" if pos.partial else "FILL",
            "requested_lots": pos.close_requested_lots or pos.quantity,
            "executed_lots": executed_lots,
            "execution_price": f"{pos.entry_price:.4f}",
            "exit_reason": pos.close_reason or "",
            "exit_price": f"{exit_price:.4f}",
            "exit_time": datetime.now(timezone.utc).isoformat(),
            "commission_rub": f"{total_commission:.2f}",
            "commission_source": commission_source,
            "actual_commission_rub": f"{pos.actual_commission:.2f}" if pos.actual_commission is not None else "",
            "entry_commission_rub": f"{entry_commission:.2f}",
            "exit_commission_rub": f"{exit_commission:.2f}",
            "slippage_rub": f"{total_slippage:+.2f}",
            "slippage_percent": f"{slippage_percent:+.3f}",
            "gross_pnl_rub": f"{gross_pnl:.2f}",
            "net_pnl_rub": f"{net_pnl:.2f}",
            "cumulative_net_pnl_rub": f"{cumulative_after:.2f}",
            "error_note": "",
        }

        if research_logger.log_trade(trade_row):
            pos.logged = True
            logging.info(
                f"✅ [CLOSED] {pos.ticker} {pos.close_reason} "
                f"entry={pos.entry_price:.4f} exit={exit_price:.4f} "
                f"lots={executed_lots} gross={gross_pnl:+.2f}₽ "
                f"net={net_pnl:+.2f}₽ cum={cumulative_after:+.2f}₽"
            )
            try:
                research_logger.log_event("CLOSED", {
                    "trade_id": trade_id, "ticker": pos.ticker, "figi": pos.figi,
                    "entry_price": pos.entry_price, "exit_price": exit_price,
                    "executed_lots": executed_lots, "lot_size": pos.lot_size,
                    "gross_pnl": gross_pnl,
                    "entry_commission": entry_commission,
                    "exit_commission": exit_commission,
                    "commission_source": commission_source,
                    "slippage": total_slippage, "slippage_percent": slippage_percent,
                    "net_pnl": net_pnl, "cumulative_net_pnl": cumulative_after,
                    "exit_reason": pos.close_reason or "",
                })
            except Exception as e:
                logging.warning(f"[jsonl] {pos.ticker}: не удалось записать CLOSED: {type(e).__name__}: {e}")
            await notify(
                f"🔒 <b>{pos.ticker}</b> закрыто по {pos.close_reason}\n"
                f"Вход: {pos.entry_price:.4f} → Выход: {exit_price:.4f}\n"
                f"Лотов: {executed_lots} (акций: {shares})\n"
                f"Net: <b>{net_pnl:+.2f} ₽</b>"
            )
        else:
            logging.error(f"[csv] {pos.ticker}: log_trade вернул False — повтор на следующем цикле.")


# ============================================================
# TRADING CYCLE
# ============================================================

async def trading_cycle(client: TinkoffClient, rm: RiskManager, sm: StrategyManager, monitor: ExecutionMonitor):
    global reconcile_done

    # 1. Цены: TOP-10 + открытые позиции
    figi_set = set(p.figi for p in monitor.positions.values())
    figi_set.update(i['figi'] for i in top_instruments)
    figi_to_monitor = list(figi_set)
    prices = await client.get_last_prices(figi_to_monitor)

    if prices is None:
        logging.error("[cycle] prices=None — monitoring_error=True, мониторинг продолжается")
        monitor.monitoring_error = True
        prices = {}

    # 2. SL/TP
    to_close = await monitor.check_all(prices)
    for pos in to_close:
        price = prices.get(pos.figi)
        if price is None or price <= 0:
            logging.error(f"[cycle] {pos.ticker}: нет цены для закрытия — пропуск (UNCERTAIN)")
            pos.state = PositionState.UNCERTAIN
            client.uncertain_state = True
            continue
        await close_position(client, pos, price)

    # 3. Обработка закрытий и UNCERTAIN
    await process_closing(client, monitor)

    # 4. Если uncertain — reconcile
    if client.is_uncertain():
        logging.warning("[cycle] uncertain_state=True — пробую reconcile.")
        await reconcile(client, monitor)
        if client.is_uncertain():
            logging.warning("[cycle] reconcile не снял UNCERTAIN — новые входы запрещены.")
            _save_state(monitor, rm)
            return

    # 5. Блокировки входов
    if not is_trading_enabled():
        logging.info("[cycle] Торговля выключена (нет trading.flag).")
        _save_state(monitor, rm)
        return
    if not reconcile_done:
        logging.warning("[cycle] reconcile не завершён.")
        _save_state(monitor, rm)
        return
    if monitor.monitoring_error:
        logging.warning("[cycle] monitoring_error=True — новые входы запрещены.")
        _save_state(monitor, rm)
        return

    # 6. Свежий портфель перед входами
    positions = await client.get_positions()
    if positions is None:
        logging.error("[cycle] Портфель недостоверен перед входом — новые входы запрещены.")
        client.uncertain_state = True
        reconcile_done = False
        _save_state(monitor, rm)
        return

    # 7. Входы
    for inst in top_instruments:
        figi = inst['figi']
        price = prices.get(figi)
        if price is None or price <= 0:
            logging.info(f"{inst['ticker']}: нет цены, пропуск")
            continue
        pos_data = positions.get(figi)
        if (pos_data is not None and pos_data.lots != 0) or monitor.has_active_position(figi):
            logging.info(f"{inst['ticker']}: позиция уже открыта/в неопределённости, пропуск")
            continue

        candles = await client.get_historical_candles(
            figi, days=config.CANDLES_LOOKBACK_DAYS,
            interval=CandleInterval.CANDLE_INTERVAL_5_MIN,
        )
        if len(candles) < config.MIN_CANDLES_FOR_SIGNAL:
            logging.info(f"{inst['ticker']}: мало свечей ({len(candles)}/{config.MIN_CANDLES_FOR_SIGNAL}), пропуск")
            continue

        df = pd.DataFrame([{
            'time': c.time,
            'open': float(c.open.units + c.open.nano / 1e9),
            'high': float(c.high.units + c.high.nano / 1e9),
            'low': float(c.low.units + c.low.nano / 1e9),
            'close': float(c.close.units + c.close.nano / 1e9),
            'volume': int(c.volume),
            'is_complete': getattr(c, 'is_complete', None),
        } for c in candles])
        df = df.set_index('time').sort_index()

        signal, reason = sm.get_signal(df, float(price), drop_incomplete=True)

        if signal == "HOLD":
            logging.info(f"{inst['ticker']}: HOLD ({reason})")
            _log_strategy_diagnostic(
                event_type="STRATEGY_HOLD", ticker=inst['ticker'], figi=figi,
                signal=signal, reason=reason, sm=sm, df=df, price=float(price),
            )
            continue

        if signal == "SELL":
            logging.info(f"{inst['ticker']}: SELL без позиции — шорт запрещён")
            _log_strategy_diagnostic(
                event_type="STRATEGY_SELL_IGNORED", ticker=inst['ticker'], figi=figi,
                signal=signal, reason=reason, sm=sm, df=df, price=float(price),
            )
            continue

        lots = await rm.calculate_position_size(figi, float(price), df)
        if lots <= 0:
            logging.info(f"{inst['ticker']}: размер позиции 0, пропуск")
            continue

        info = await client.get_instrument_info(figi)
        if not info:
            logging.error(f"{inst['ticker']}: instrument_info=None — пропуск")
            continue

        trade_id = uuid.uuid4().hex[:12]
        order_send_time = datetime.now(timezone.utc).isoformat()
        research_logger.log_event("SIGNAL", {
            "trade_id": trade_id, "ticker": inst['ticker'], "figi": figi,
            "signal": signal, "reason": reason, "price_before_order": float(price),
            "requested_lots": lots, "lot_size": info['lot'],
        })

        result = await client.post_market_order(figi, lots, OrderDirection.ORDER_DIRECTION_BUY)

        if result.state is None:
            logging.error(
                f"{inst['ticker']}: state=None (order_id={result.order_id}) — UNCERTAIN. "
                f"Повторная отправка запрещена."
            )
            client.uncertain_state = True
            research_logger.log_event("BUY_UNKNOWN", {
                "trade_id": trade_id, "ticker": inst['ticker'], "figi": figi,
                "order_id": result.order_id,
            })
            _save_state(monitor, rm)
            continue

        status = getattr(result.state, 'execution_report_status', None)
        order_id = result.order_id

        # --- PARTIALLYFILL ---
        if status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL:
            logging.error(
                f"{inst['ticker']}: ЧАСТИЧНОЕ исполнение покупки — отмена остатка "
                f"(order_id={order_id})"
            )
            research_logger.log_event("PARTIAL_FILL", {"trade_id": trade_id, "order_id": order_id})
            client.uncertain_state = True

            cancel_resp = await client.cancel_order(order_id)
            if cancel_resp is None:
                logging.error(f"{inst['ticker']}: cancel_order вернул None — UNCERTAIN")
                _save_state(monitor, rm)
                continue

            final_state = await client.get_sandbox_order_state(order_id)
            if final_state is None:
                logging.error(f"{inst['ticker']}: final state=None — UNCERTAIN")
                _save_state(monitor, rm)
                continue

            fresh_positions = await client.get_positions()
            if fresh_positions is None:
                logging.error(f"{inst['ticker']}: fresh positions=None — UNCERTAIN")
                _save_state(monitor, rm)
                continue

            pos_data = fresh_positions.get(figi)
            if pos_data is None or pos_data.lots == 0:
                logging.warning(f"{inst['ticker']}: после отмены позиции нет — OK")
                _save_state(monitor, rm)
                continue

            le = pos_data.lots
            exec_price = ExecutionMonitor._extract_price(final_state, le, info['lot'])

            if exec_price is None:
                logging.error(f"{inst['ticker']}: exec_price=None после partial — UNCERTAIN")
                _save_state(monitor, rm)
                continue

            sl_price, tp_price = rm.calculate_sl_tp(exec_price, signal, df)
            if sl_price is None or tp_price is None:
                logging.error(f"{inst['ticker']}: SL/TP не рассчитаны — UNCERTAIN")
                _save_state(monitor, rm)
                continue

            pos = monitor.register(
                figi=figi, ticker=inst['ticker'], direction=signal,
                entry_price=exec_price, quantity=le,
                sl_price=sl_price, tp_price=tp_price, lot_size=info['lot'],
                trade_id=trade_id, signal_reason=reason,
                signal_time=order_send_time, price_before_order=float(price),
                order_send_time=order_send_time,
                entry_requested_lots=lots,
                entry_applied_lots=le,
                entry_order_id=order_id,
            )
            pos.execution_price = exec_price
            pos.executed_lots = le
            pos.partial = True
            pos.entry_actual_commission = ExecutionMonitor._extract_commission(final_state)

            rm.record_trade(figi)
            _save_state(monitor, rm)
            logging.warning(
                f"⚠️ [PARTIAL] {inst['ticker']}: {le} лот. @ {exec_price:.4f}, "
                f"заявка {order_id} отменена"
            )
            await notify(
                f"⚠️ <b>{inst['ticker']}</b> частичное исполнение BUY\n"
                f"Исполнено: {le} лот. @ {exec_price:.4f}\n"
                f"Остаток отменён."
            )
            continue

        # --- НЕ FILL (этап 4.1: проверка фактического исполнения) ---
        if status != OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL:
            logging.warning(
                f"{inst['ticker']}: не исполнено ({status}) — проверка фактического "
                f"исполнения (order_id={order_id})"
            )
            research_logger.log_event("ORDER_NOT_FILLED", {
                "trade_id": trade_id, "ticker": inst['ticker'], "figi": figi,
                "order_id": order_id, "status": str(status),
                "requested_lots": lots,
            })

            if not order_id:
                logging.error(f"{inst['ticker']}: нет order_id — UNCERTAIN")
                client.uncertain_state = True
                _save_state(monitor, rm)
                continue

            # Запросить фактическое состояние заявки
            state = await client.get_sandbox_order_state(order_id)
            if state is None:
                logging.error(
                    f"{inst['ticker']}: state=None после не-FILL (order_id={order_id}) — UNCERTAIN"
                )
                client.uncertain_state = True
                _save_state(monitor, rm)
                continue

            le = int(getattr(state, "lots_executed", 0) or 0)
            if le <= 0:
                logging.warning(
                    f"{inst['ticker']}: lots_executed=0 — заявка не исполнена, пропуск"
                )
                _save_state(monitor, rm)
                continue

            # Исполнено > 0 -> создать TrackedPosition (как при FILL)
            exec_price = ExecutionMonitor._extract_price(state, le, info['lot'])
            if exec_price is None:
                logging.error(
                    f"{inst['ticker']}: exec_price=None (lots_executed={le}) — UNCERTAIN"
                )
                client.uncertain_state = True
                _save_state(monitor, rm)
                continue

            entry_commission = ExecutionMonitor._extract_commission(state)
            sl_price, tp_price = rm.calculate_sl_tp(exec_price, signal, df)
            if sl_price is None or tp_price is None:
                logging.error(f"{inst['ticker']}: SL/TP не рассчитаны — UNCERTAIN")
                client.uncertain_state = True
                _save_state(monitor, rm)
                continue

            pos = monitor.register(
                figi=figi, ticker=inst['ticker'], direction=signal,
                entry_price=exec_price, quantity=le,
                sl_price=sl_price, tp_price=tp_price, lot_size=info['lot'],
                trade_id=trade_id, signal_reason=reason,
                signal_time=order_send_time, price_before_order=float(price),
                order_send_time=order_send_time,
                entry_requested_lots=lots,
                entry_applied_lots=le,
                entry_order_id=order_id,
            )
            pos.execution_price = exec_price
            pos.executed_lots = le
            pos.entry_actual_commission = entry_commission

            rm.record_trade(figi)
            _save_state(monitor, rm)
            logging.warning(
                f"⚠️ [NOT-FILL-EXEC] {inst['ticker']}: status={status}, "
                f"lots_executed={le} — создан TrackedPosition @ {exec_price:.4f}"
            )
            research_logger.log_event("POSITION_OPENED", {
                "trade_id": trade_id, "ticker": inst['ticker'], "figi": figi,
                "order_id": order_id, "exec_price": exec_price,
                "exec_lots": le, "sl": sl_price, "tp": tp_price,
                "entry_commission": entry_commission,
                "note": f"status={status}, lots_executed={le}",
            })
            await notify(
                f"⚠️ <b>{inst['ticker']}</b> BUY (status={status})\n"
                f"Исполнено: {le} лот. @ {exec_price:.4f}\n"
                f"SL: {sl_price:.4f}\nTP: {tp_price:.4f}\n"
                f"<i>{reason}</i>"
            )
            continue

        # --- FILL ---
        state = await client.get_sandbox_order_state(order_id) if order_id else None
        if state is None:
            logging.error(f"{inst['ticker']}: state=None после FILL — UNCERTAIN")
            client.uncertain_state = True
            _save_state(monitor, rm)
            continue

        le = int(getattr(state, 'lots_executed', 0) or 0)
        if le <= 0:
            logging.error(f"{inst['ticker']}: FILL, но lots_executed={le} — UNCERTAIN")
            client.uncertain_state = True
            _save_state(monitor, rm)
            continue

        exec_price = ExecutionMonitor._extract_price(state, le, info['lot'])
        if exec_price is None:
            logging.error(f"{inst['ticker']}: exec_price=None — UNCERTAIN")
            client.uncertain_state = True
            _save_state(monitor, rm)
            continue

        entry_commission = ExecutionMonitor._extract_commission(state)
        rm.record_trade(figi)
        sl_price, tp_price = rm.calculate_sl_tp(exec_price, signal, df)
        if sl_price is None or tp_price is None:
            logging.error(f"{inst['ticker']}: SL/TP не рассчитаны — UNCERTAIN")
            client.uncertain_state = True
            _save_state(monitor, rm)
            continue

        pos = monitor.register(
            figi=figi, ticker=inst['ticker'], direction=signal,
            entry_price=exec_price, quantity=le,
            sl_price=sl_price, tp_price=tp_price, lot_size=info['lot'],
            trade_id=trade_id, signal_reason=reason,
            signal_time=order_send_time, price_before_order=float(price),
            order_send_time=order_send_time,
            entry_requested_lots=lots,
            entry_applied_lots=le,
            entry_order_id=order_id,
        )
        pos.execution_price = exec_price
        pos.executed_lots = le
        pos.entry_actual_commission = entry_commission

        _save_state(monitor, rm)
        logging.info(
            f"🚀 [BUY] {inst['ticker']} {le} лот. @ {exec_price:.4f} "
            f"SL={sl_price:.4f} TP={tp_price:.4f} "
            f"entry_comm={entry_commission if entry_commission is not None else 'model'} "
            f"(reason: {reason})"
        )
        research_logger.log_event("POSITION_OPENED", {
            "trade_id": trade_id, "ticker": inst['ticker'], "figi": figi,
            "order_id": order_id, "exec_price": exec_price,
            "exec_lots": le, "sl": sl_price, "tp": tp_price,
            "entry_commission": entry_commission,
        })
        await notify(
            f"🚀 <b>{inst['ticker']}</b> BUY\n"
            f"Исполнено: {le} лот. @ {exec_price:.4f}\n"
            f"SL: {sl_price:.4f}\nTP: {tp_price:.4f}\n"
            f"<i>{reason}</i>"
        )


# ============================================================
# MAIN
# ============================================================

async def main():
    global telegram_bot, main_client, top_instruments, monitor_ref, rm_ref

    config.validate()

    if config.ENABLE_TELEGRAM and TelegramBot:
        try:
            telegram_bot = TelegramBot(
                config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID,
                config.TELEGRAM_ALLOWED_USER_ID,
                {'start_trading': start_trading, 'stop_trading': stop_trading, 'get_status': get_status},
            )
            await asyncio.wait_for(telegram_bot.application.initialize(), timeout=20.0)
            await asyncio.wait_for(telegram_bot.application.start(), timeout=20.0)
            await asyncio.wait_for(telegram_bot.application.updater.start_polling(), timeout=20.0)
            await telegram_bot.register_commands()
            logging.info("Telegram-бот запущен (initialize → start → polling → commands).")
            await notify("🤖 Tsclp_v2.2_volatile запущен. Управление — через trading.flag.")
        except Exception as e:
            logging.warning(
                f"Telegram недоступен ({type(e).__name__}: {e!r}). "
                f"Робот работает БЕЗ уведомлений."
            )
            await _stop_telegram_bot()
        asyncio.create_task(_telegram_watchdog())
        logging.info("[tg-watchdog] Сторож запущен (60 сек, полный перезапуск при падении).")

    async with TinkoffClient(config.TINKOFF_API_TOKEN, config.USE_SANDBOX) as client:
        main_client = client
        rm = RiskManager(client)
        rm_ref = rm
        sm = StrategyManager()
        monitor = ExecutionMonitor(client)
        monitor_ref = monitor
        screener = Screener()

        state_loaded = _load_state(monitor, rm)
        if not state_loaded:
            client.uncertain_state = True
            logging.warning(
                "[main] state.json отсутствует/повреждён — новые входы запрещены до сверки."
            )

        instruments = await screener.select_top_volatile()
        if not instruments:
            client.uncertain_state = True
            msg = "⚠️ Скрининг не удался. Новые входы ЗАПРЕЩЕНЫ."
            logging.error(msg)
            research_logger.log_event("SCREEN_FAILED", {})
            await notify(msg)
            while True:
                await asyncio.sleep(60)

        top_instruments = instruments
        logging.info(f"[screener] Инструменты: {', '.join(i['ticker'] for i in instruments)}")

        await reconcile(client, monitor)

        if not reconcile_done:
            logging.warning("[main] reconcile не завершён — новые входы запрещены.")
            client.uncertain_state = True

        while True:
            try:
                await trading_cycle(client, rm, sm, monitor)
            except Exception as e:
                logging.exception(f"Ошибка цикла: {type(e).__name__}: {e}")
                research_logger.log_event("CYCLE_ERROR", {"error": str(e)})
            await asyncio.sleep(config.TRADING_CYCLE_SECONDS)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Остановлено пользователем.")
    except Exception as e:
        logging.critical(f"Фатальная ошибка: {type(e).__name__}: {e}")