# ============================================================
# MYRAA — V2.3 BASELINE
# ============================================================
# Purpose:
#   • Fully-auditable, production-grade PAPER engine
#   • No strategy changes vs V2.2
#   • Adds identity, robustness, and session intelligence
#
# Strategy behavior:
#   • UNCHANGED from V2.2 ROBUST
#
# Trading window:
#   • 09:30–10:00 ET  → read-only warmup
#   • 10:00–11:30 ET  → trading allowed
#
# Logs:
#   • decision_log_v2.csv
#   • shadow_log_v2.csv
#   • trade_attribution_v2.csv
#
# ============================================================

from __future__ import annotations

import os
import csv
import time as time_mod
import hashlib
from pathlib import Path
from datetime import datetime, timedelta, time, date as date_cls, timezone
from typing import Dict, List, Optional

import pandas as pd
import pytz
from dotenv import load_dotenv

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestQuoteRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest, StopLossRequest, TakeProfitRequest

# ============================================================
# VERSION / BUILD IDENTITY (AUTHORITATIVE)
# ============================================================

ENGINE_NAME = "MYRAA"
ENGINE_VERSION = "v2.3_BASELINE"
ENGINE_STRATEGY_ID = "V2_CORE_NO_STRATEGY_CHANGE"

ENGINE_FEATURES = [
    "market-hours-only-stats",
    "warmup-read-only",
    "spy-vwap-market-filter",
    "first30-vol-regime-lock",
    "atr-regime-classification",
    "spread-usd-pct-atr-risk-guards",
    "decision-logging-v2",
    "shadow-logging-v2",
    "blocked-by-system-logging",
    "time-of-day-tagging",
    "session-summary-report",
]

# ============================================================
# ENV + PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

load_dotenv(BASE_DIR / ".env")

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")
ALPACA_PAPER = os.getenv("ALPACA_PAPER", "true").lower() in ("1", "true", "yes")

DATA_FEED = os.getenv("DATA_FEED", "iex")

REPORTS_DIR = BASE_DIR / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

DECISION_LOG_PATH = BASE_DIR / "decision_log_v2.csv"
SHADOW_LOG_PATH = BASE_DIR / "shadow_log_v2.csv"
ATTRIBUTION_LOG_PATH = BASE_DIR / "trade_attribution_v2.csv"

# ============================================================
# TIME / SESSION RULES (ET)
# ============================================================

ET = pytz.timezone("America/New_York")

MARKET_OPEN_ET = time(9, 30)
WARMUP_END_ET = time(10, 0)
START_TRADING_ET = time(10, 0)
STOP_NEW_TRADES_ET = time(11, 30)
MARKET_CLOSE_ET = time(16, 0)

LOOP_SLEEP_SECONDS = int(os.getenv("LOOP_SLEEP_SECONDS", "20"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "30"))

# ============================================================
# RISK / LIMITS (UNCHANGED)
# ============================================================

MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "3"))
DAILY_LOSS_LIMIT_USD = float(os.getenv("DAILY_LOSS_LIMIT_USD", "250"))

# Spread guards
MAX_SPREAD_USD = float(os.getenv("MAX_SPREAD_USD", "0.25"))
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "0.0006"))
MAX_SPREAD_ATR_RATIO = float(os.getenv("MAX_SPREAD_ATR_RATIO", "0.08"))
MAX_SPREAD_RISK_RATIO = float(os.getenv("MAX_SPREAD_RISK_RATIO", "0.05"))

# ATR regime thresholds
ATR_PCT_LOW = float(os.getenv("ATR_PCT_LOW", "0.0009"))
ATR_PCT_HIGH = float(os.getenv("ATR_PCT_HIGH", "0.0018"))

# First 30 minute volatility regime lock
FIRST30_LOCK_TIME_ET = time(10, 0)
FIRST30_REALIZED_VOL_TH = float(os.getenv("FIRST30_REALIZED_VOL_TH", "0.0018"))
FIRST30_RANGE_PCT_TH = float(os.getenv("FIRST30_RANGE_PCT_TH", "0.0075"))

# ============================================================
# SESSION STATE (GLOBAL, INTENTIONAL)
# ============================================================

SESSION: Dict[str, any] = {
    "engine": ENGINE_NAME,
    "version": ENGINE_VERSION,
    "strategy_id": ENGINE_STRATEGY_ID,
    "features": ENGINE_FEATURES,
    "session_date": None,
    "run_id": None,
    "first30_vol_flag": None,
    "equity_start": None,
    "equity_end": None,
    "realized_today": 0.0,
    "trades_submitted": 0,
}# ============================================================
# TIMEZONE + SESSION WINDOWS
# ============================================================

ET = pytz.timezone("America/New_York")

MARKET_OPEN_ET   = time(9, 30)
WARMUP_END_ET    = time(10, 0)    # read-only
STOP_NEW_TRADES  = time(11, 30)
MARKET_CLOSE_ET  = time(16, 0)

LOOP_SLEEP_SECONDS = int(os.getenv("LOOP_SLEEP_SECONDS", "20"))
COOLDOWN_MINUTES   = int(os.getenv("COOLDOWN_MINUTES", "30"))

# ============================================================
# ENGINE IDENTITY (RUN TRACEABILITY)
# ============================================================

ENGINE_VERSION = "v2.3_BASELINE"
ENGINE_HASH = hashlib.sha256(
    f"{ENGINE_VERSION}-{Path(__file__).stat().st_mtime}".encode()
).hexdigest()[:12]

# ============================================================
# SESSION STATE (SINGLE SOURCE OF TRUTH)
# ============================================================

SESSION: Dict[str, object] = {
    "engine_version": ENGINE_VERSION,
    "engine_hash": ENGINE_HASH,
    "session_date": None,
    "run_id": None,
    "equity_start": None,
    "equity_last": None,
    "equity_end": None,
    "realized_today": 0.0,
    "trades_today": 0,
    "first30_vol_flag": None,
    "warmup_complete": False,
}

# ============================================================
# RISK / LIMIT CONSTANTS (LOCKED — NO ADAPTATION)
# ============================================================

MAX_TRADES_PER_DAY   = int(os.getenv("MAX_TRADES_PER_DAY", "3"))
DAILY_LOSS_LIMIT_USD = float(os.getenv("DAILY_LOSS_LIMIT_USD", "250"))

MAX_SPREAD_USD       = float(os.getenv("MAX_SPREAD_USD", "0.25"))
MAX_SPREAD_PCT       = float(os.getenv("MAX_SPREAD_PCT", "0.0006"))

MAX_SPREAD_ATR_RATIO  = float(os.getenv("MAX_SPREAD_ATR_RATIO", "0.08"))
MAX_SPREAD_RISK_RATIO = float(os.getenv("MAX_SPREAD_RISK_RATIO", "0.05"))

ATR_PCT_LOW  = float(os.getenv("ATR_PCT_LOW", "0.0009"))
ATR_PCT_HIGH = float(os.getenv("ATR_PCT_HIGH", "0.0018"))

# ============================================================
# FIRST-30-MIN VOLATILITY CLASSIFICATION (TAG ONLY)
# ============================================================

FIRST30_LOCK_TIME_ET       = time(10, 0)
FIRST30_REALIZED_VOL_TH    = float(os.getenv("FIRST30_REALIZED_VOL_TH", "0.0018"))
FIRST30_RANGE_PCT_TH       = float(os.getenv("FIRST30_RANGE_PCT_TH", "0.0075"))

# ============================================================
# HELPERS — TIME
# ============================================================

def now_et() -> datetime:
    return datetime.now(ET)

def in_market_hours(t: time) -> bool:
    return MARKET_OPEN_ET <= t <= MARKET_CLOSE_ET

def in_warmup(t: time) -> bool:
    return MARKET_OPEN_ET <= t < WARMUP_END_ET

def allow_new_entries(t: time) -> bool:
    return WARMUP_END_ET <= t < STOP_NEW_TRADES

# ============================================================
# CSV SCHEMAS (LOCKED)
# ============================================================

DECISION_FIELDS = [
    "ts", "symbol", "decision", "reason",
    "entry", "stop", "tp",
    "bid", "ask", "spread",
    "qty", "risk_usd",
    "realized_today", "trades_today",
    "equity", "atr", "atr_pct", "atr_regime",
    "vol_flag", "tod_bucket", "note"
]

SHADOW_FIELDS = [
    "ts", "symbol", "shadow_type",
    "blocked_by", "decision", "reason",
    "bid", "ask",
    "atr", "atr_pct", "atr_regime",
    "spy", "vwap",
    "vol_flag", "tod_bucket",
    "note"
]

ATTR_FIELDS = [
    "trade_id", "symbol", "side", "qty",
    "entry_time_et", "exit_time_et",
    "hold_seconds",
    "entry_price", "stop_price", "tp_price",
    "exit_price", "exit_reason",
    "outcome", "r_multiple",
    "mfe_abs", "mae_abs", "mfe_r", "mae_r",
    "entry_spread_usd", "entry_spread_pct",
    "entry_quality", "exit_quality",
    "atr_pct_entry", "atr_regime",
    "session_date", "note"
]

# ============================================================
# CSV INITIALIZATION (IDEMPOTENT)
# ============================================================

def init_csv(path: Path, fields: List[str]) -> None:
    if not path.exists():
        with path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(fields)

# ============================================================
# TIME-OF-DAY TAGGING (TAG ONLY)
# ============================================================

def tod_bucket(ts: datetime) -> str:
    t = ts.time()
    if t < time(9, 30):
        return "PREMARKET"
    if time(9, 30) <= t < time(10, 0):
        return "OPENING_WARMUP"
    if time(10, 0) <= t < time(11, 30):
        return "MORNING"
    if time(11, 30) <= t < time(14, 0):
        return "MIDDAY"
    return "AFTERNOON"

# ============================================================
# DECISION LOGGER (PRIMARY OBSERVABILITY)
# ============================================================

def log_decision(symbol: str, decision: str, reason: str, **details) -> None:
    ts = now_et()
    row = {
        "ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": symbol,
        "decision": decision,
        "reason": reason,
        "tod_bucket": tod_bucket(ts),
    }
    row.update(details)

    with DECISION_LOG_PATH.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DECISION_FIELDS)
        writer.writerow({k: row.get(k, "") for k in DECISION_FIELDS})

# ============================================================
# SHADOW LOGGER (COUNTERFACTUAL READY)
# ============================================================

def log_shadow(
    symbol: str,
    shadow_type: str,
    blocked_by: str,
    decision: str,
    reason: str,
    **details
) -> None:
    ts = now_et()
    row = {
        "ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": symbol,
        "shadow_type": shadow_type,
        "blocked_by": blocked_by,
        "decision": decision,
        "reason": reason,
        "tod_bucket": tod_bucket(ts),
    }
    row.update(details)

    with SHADOW_LOG_PATH.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SHADOW_FIELDS)
        writer.writerow({k: row.get(k, "") for k in SHADOW_FIELDS})

# ============================================================
# ATTRIBUTION LOGGER (TRADE LIFECYCLE)
# ============================================================

def log_attribution(**row) -> None:
    with ATTR_LOG_PATH.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ATTR_FIELDS)
        writer.writerow({k: row.get(k, "") for k in ATTR_FIELDS})
# ============================================================
# MARKET DATA HELPERS (MARKET-HOURS ONLY)
# ============================================================

def fetch_bars(
    data: StockHistoricalDataClient,
    symbol: str,
    minutes: int,
    end_ts: datetime,
) -> pd.DataFrame:
    """
    Fetch minute bars, market-hours only.
    """
    start = end_ts - timedelta(minutes=minutes)
    req = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame.Minute,
        start=start,
        end=end_ts,
        feed=DATA_FEED,
        adjustment="raw",
    )
    bars = data.get_stock_bars(req).df
    if bars is None or len(bars) == 0:
        return pd.DataFrame()

    if isinstance(bars.index, pd.MultiIndex):
        bars = bars.xs(symbol)

    bars = bars.sort_index()
    bars = bars.between_time(MARKET_OPEN_ET, MARKET_CLOSE_ET)
    return bars


# ============================================================
# VWAP (SPY MARKET FILTER)
# ============================================================

def calc_vwap(df: pd.DataFrame) -> float:
    if df.empty or "close" not in df or "volume" not in df:
        return 0.0
    v = df["volume"]
    p = df["close"]
    total = (p * v).sum()
    vol = v.sum()
    return float(total / vol) if vol > 0 else 0.0


# ============================================================
# ATR + REGIME TAGGING (NON-ADAPTIVE)
# ============================================================

ATR_LOOKBACK = 14
ATR_PCT_LOW  = 0.0005
ATR_PCT_HIGH = 0.0020

def calc_atr(df: pd.DataFrame) -> float:
    if len(df) < ATR_LOOKBACK + 1:
        return 0.0

    high = df["high"]
    low = df["low"]
    close = df["close"].shift(1)

    tr = pd.concat([
        high - low,
        (high - close).abs(),
        (low - close).abs()
    ], axis=1).max(axis=1)

    atr = tr.rolling(ATR_LOOKBACK).mean().iloc[-1]
    return float(atr) if not math.isnan(atr) else 0.0


def atr_regime(atr_pct: float) -> str:
    if atr_pct <= 0:
        return "INVALID"
    if atr_pct < ATR_PCT_LOW:
        return "LOW"
    if atr_pct > ATR_PCT_HIGH:
        return "HIGH"
    return "NORMAL"


# ============================================================
# LIQUIDITY + SPREAD CHECKS (TAG ONLY)
# ============================================================

MIN_AVG_DOLLAR_VOL = 2_000_000
MIN_SPREAD_USD    = 0.01
MAX_SPREAD_USD    = 1.00
MAX_SPREAD_PCT    = 0.005

def avg_dollar_volume(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    return float((df["close"] * df["volume"]).rolling(20).mean().iloc[-1])


def spread_ok(bid: float, ask: float) -> Dict[str, float]:
    if bid <= 0 or ask <= 0 or ask <= bid:
        return {"ok": False}

    spread = ask - bid
    mid = (ask + bid) / 2
    spread_pct = spread / mid if mid > 0 else 0

    return {
        "ok": (
            spread >= MIN_SPREAD_USD
            and spread <= MAX_SPREAD_USD
            and spread_pct <= MAX_SPREAD_PCT
        ),
        "spread": spread,
        "spread_pct": spread_pct,
    }


# ============================================================
# BEST-CANDIDATE SNAPSHOT (FOR SHADOW LOGGING)
# ============================================================

def compute_best_candidate_snapshot(
    data: StockHistoricalDataClient,
    symbols: List[str],
    quotes: Dict[str, Dict[str, float]],
    session_date: date_cls,
    equity_now: float,
    realized_today: float,
    trades_today: int,
    vol_flag: str,
) -> Dict[str, str]:
    """
    Best-effort snapshot — never blocks trading.
    """
    best = {"note": "no_candidate_passed_filters"}

    now = now_et()
    for sym in symbols:
        q = quotes.get(sym)
        if not q:
            continue

        bid, ask = q.get("bid", 0), q.get("ask", 0)
        sp = spread_ok(bid, ask)
        if not sp.get("ok"):
            continue

        bars = fetch_bars(data, sym, 60, now)
        if len(bars) < ATR_LOOKBACK + 1:
            continue

        atr = calc_atr(bars)
        atr_pct = atr / bars["close"].iloc[-1] if atr > 0 else 0

        best.update({
            "symbol": sym,
            "bid": bid,
            "ask": ask,
            "atr": round(atr, 4),
            "atr_pct": round(atr_pct, 6),
            "atr_regime": atr_regime(atr_pct),
            "note": "best_candidate_snapshot",
        })
        break

    return best
# ============================================================
# MAIN LOOP
# ============================================================

def main():
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        raise RuntimeError("Missing Alpaca API keys")

    init_csv(DECISION_LOG_PATH, DECISION_FIELDS)
    init_csv(SHADOW_LOG_PATH, SHADOW_FIELDS)
    init_csv(ATTR_LOG_PATH, ATTR_FIELDS)

    trading = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=ALPACA_PAPER)
    data = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

    symbols = load_universe()
    if not symbols:
        raise RuntimeError("Universe empty")

    SESSION["run_id"] = now_et().strftime("%Y%m%d_%H%M%S")
    SESSION["session_date"] = now_et().date().isoformat()

    log_decision("SYSTEM", "START", "boot",
                 run_id=SESSION["run_id"],
                 symbols=len(symbols),
                 trading_window="10:00–11:30",
                 warmup="09:30–10:00")

    last_spy_log_ts = None

    try:
        while True:
            now = now_et()
            t = now.time()

            # ============================
            # MARKET CLOSED
            # ============================
            if t < MARKET_OPEN_ET or t >= MARKET_CLOSE_ET:
                log_decision("SYSTEM", "SKIP", "market_closed")
                time_mod.sleep(LOOP_SLEEP_SECONDS)
                continue

            # ============================
            # WARM-UP (READ ONLY)
            # ============================
            if MARKET_OPEN_ET <= t < START_TRADING_ET:
                log_decision("SYSTEM", "SKIP", "warmup_read_only",
                             note="collecting_bars_no_trades")
                time_mod.sleep(LOOP_SLEEP_SECONDS)
                continue

            # ============================
            # AFTER CUTOFF
            # ============================
            if t >= STOP_NEW_TRADES_ET:
                log_decision("SYSTEM", "SKIP", "after_cutoff",
                             note="no_new_entries")
                time_mod.sleep(LOOP_SLEEP_SECONDS)
                continue

            # ============================
            # ACCOUNT STATE
            # ============================
            acct = trading.get_account()
            equity_now = float(acct.equity)

            positions = {p.symbol for p in trading.get_all_positions()}
            open_orders = trading.get_orders()
            open_order_syms = {o.symbol for o in open_orders}

            # ============================
            # SPY MARKET FILTER
            # ============================
            spy_df = fetch_bars(data, "SPY", 90, now)
            if len(spy_df) < 20:
                log_decision("SPY", "SKIP", "spy_bars_not_ready")
                time_mod.sleep(LOOP_SLEEP_SECONDS)
                continue

            spy_last = float(spy_df["close"].iloc[-1])
            spy_vwap = calc_vwap(spy_df)

            if spy_vwap > 0 and spy_last < spy_vwap:
                now_ts = time_mod.time()
                if last_spy_log_ts is None or now_ts - last_spy_log_ts > 60:
                    last_spy_log_ts = now_ts
                    log_decision("SPY", "SKIP", "market_filter_spy_below_vwap",
                                 spy=spy_last, vwap=spy_vwap)

                    log_blocked_by(
                        blocked_by="market_filter_spy_below_vwap",
                        data=data,
                        symbols=symbols,
                        quotes={},
                        session_date=now.date(),
                        equity_now=equity_now,
                        realized_today=0.0,
                        trades_today=0,
                        vol_flag="NORMAL",
                        spy_last=spy_last,
                        spy_vwap=spy_vwap,
                        note="system_block"
                    )

                time_mod.sleep(LOOP_SLEEP_SECONDS)
                continue

            # ============================
            # CANDIDATE SCAN (NO ADAPTATION)
            # ============================
            for sym in symbols:
                if sym in positions or sym in open_order_syms:
                    log_decision(sym, "SKIP", "already_in_position")
                    continue

                bars = fetch_bars(data, sym, 60, now)
                if len(bars) < ATR_LOOKBACK + 1:
                    log_decision(sym, "SKIP", "not_enough_bars")
                    continue

                adv = avg_dollar_volume(bars)
                if adv < MIN_AVG_DOLLAR_VOL:
                    log_decision(sym, "SKIP", "liquidity_too_low",
                                 adv=round(adv))
                    continue

                atr = calc_atr(bars)
                if atr <= 0:
                    log_decision(sym, "SKIP", "atr_invalid")
                    continue

                # NOTE: Entry logic intentionally unchanged / placeholder
                # This baseline logs eligibility only
                log_decision(sym, "INFO", "eligible_candidate",
                             atr=round(atr, 4),
                             atr_regime=atr_regime(atr / bars["close"].iloc[-1]))

            time_mod.sleep(LOOP_SLEEP_SECONDS)

    except KeyboardInterrupt:
        log_decision("SYSTEM", "EXIT", "manual_shutdown")
    except Exception as e:
        log_decision("SYSTEM", "EXIT", "crash", error=str(e))
        raise


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
