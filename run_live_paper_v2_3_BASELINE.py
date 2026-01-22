from __future__ import annotations

import os
import csv
import time as time_mod
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta, time, timezone, date as date_cls
from typing import Dict, Optional, List, Tuple
from collections import Counter
import json
import hashlib
import platform
import subprocess

import pandas as pd
import pytz
from dotenv import load_dotenv

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestQuoteRequest, StockLatestTradeRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderClass, OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest, StopLossRequest, TakeProfitRequest


# =============================
# LOAD ENV
# =============================
load_dotenv(".env")

# =============================
# CONFIG (V2.1)
# =============================
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")
ALPACA_PAPER = os.getenv("ALPACA_PAPER", "true").strip().lower() in ("1", "true", "yes", "y", "on")
DATA_FEED = os.getenv("DATA_FEED", "iex")

# Universe
MYRAA_SYMBOLS = os.getenv("MYRAA_SYMBOLS", "SPY,QQQ,AAPL,MSFT,NVDA")
MYRAA_SYMBOLS_FILE = os.getenv("MYRAA_SYMBOLS_FILE", "").strip()
SYMBOL_CAP = int(os.getenv("SYMBOL_CAP", "5"))

# Time
ET = pytz.timezone("America/New_York")

MARKET_OPEN_ET = time(9, 30)
MARKET_CLOSE_ET = time(16, 0)

# =========================================================
# Time (Eastern)
# =========================================================
ET = pytz.timezone("America/New_York")

MARKET_OPEN_ET = time(9, 30)
START_TRADING_ET = time(10, 0)      # trading allowed
WARMUP_END_ET = START_TRADING_ET    # read-only until trading start
STOP_NEW_TRADES_ET = time(11, 30)
MARKET_CLOSE_ET = time(16, 0)

COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "30"))
LOOP_SLEEP_SECONDS = int(os.getenv("LOOP_SLEEP_SECONDS", "20"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "30"))
LOOP_SLEEP_SECONDS = int(os.getenv("LOOP_SLEEP_SECONDS", "20"))

# Risk controls
RISK_PER_TRADE_USD = float(os.getenv("RISK_PER_TRADE_USD", "10"))
DAILY_LOSS_LIMIT_USD = float(os.getenv("DAILY_LOSS_LIMIT_USD", "25"))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "5"))
MAX_QTY = int(os.getenv("MAX_QTY", "5"))
MIN_QTY = int(os.getenv("MIN_QTY", "1"))

# Stops
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
STOP_ATR_MULT = float(os.getenv("STOP_ATR_MULT", "1.25"))
STOP_FLOOR_USD = float(os.getenv("STOP_FLOOR_USD", "0.50"))
STOP_BUFFER_USD = float(os.getenv("STOP_BUFFER_USD", "0.10"))
TP_R_MULT = float(os.getenv("TP_R_MULT", "1.5"))

# Liquidity stats
MIN_AVG_DOLLAR_VOL = float(os.getenv("MIN_AVG_DOLLAR_VOL", "2500000"))
LIQUIDITY_LOOKBACK_MIN = int(os.getenv("LIQUIDITY_LOOKBACK_MIN", "120"))
MIN_BARS_REQUIRED = int(os.getenv("MIN_BARS_REQUIRED", "60"))

# Spread checks
MAX_SPREAD_USD = float(os.getenv("MAX_SPREAD_USD", "0.25"))
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "0.0006"))
MIN_SPREAD_BID_USD = float(os.getenv("MIN_SPREAD_BID_USD", "0.01"))

# First30 vol flag → tighter spread
MAX_SPREAD_USD_HIGH = float(os.getenv("MAX_SPREAD_USD_HIGH", "0.15"))
MAX_SPREAD_PCT_HIGH = float(os.getenv("MAX_SPREAD_PCT_HIGH", "0.0004"))

FIRST30_REALIZED_VOL_TH = float(os.getenv("FIRST30_REALIZED_VOL_TH", "0.0018"))
FIRST30_RANGE_PCT_TH = float(os.getenv("FIRST30_RANGE_PCT_TH", "0.0075"))
FIRST30_LOCK_TIME_ET = time(10, 0)

# Spread ratios
MAX_SPREAD_ATR_RATIO = float(os.getenv("MAX_SPREAD_ATR_RATIO", "0.08"))
MAX_SPREAD_RISK_RATIO = float(os.getenv("MAX_SPREAD_RISK_RATIO", "0.05"))

# SPY VWAP market filter
SPY_VWAP_LOOKBACK_MIN = int(os.getenv("SPY_VWAP_LOOKBACK_MIN", "120"))
SPY_THROTTLE_SECONDS = int(os.getenv("SPY_THROTTLE_SECONDS", "120"))

# ATR regime tags (tag-only)
ATR_PCT_LOW = float(os.getenv("ATR_PCT_LOW", "0.0009"))
ATR_PCT_HIGH = float(os.getenv("ATR_PCT_HIGH", "0.0018"))


# =============================
# PATHS
# =============================
BASE_DIR = Path(__file__).resolve().parent
REPORTS_DIR = BASE_DIR / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

DECISION_LOG_PATH = BASE_DIR / "decision_log_v2.csv"
ATTRIBUTION_LOG_PATH = BASE_DIR / "trade_attribution_v2.csv"
SHADOW_LOG_PATH = BASE_DIR / "shadow_log_v2.csv"

# =============================
# ROBUSTNESS / DIAGNOSTICS
# =============================
RUN_MANIFEST_PATH = BASE_DIR / "run_manifest_v2_3.json"
HEARTBEAT_SECONDS = int(os.getenv("HEARTBEAT_SECONDS", "180"))
RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "3"))
RETRY_SLEEP_SECONDS = float(os.getenv("RETRY_SLEEP_SECONDS", "0.6"))
MAX_BAR_STALENESS_SECONDS = int(os.getenv("MAX_BAR_STALENESS_SECONDS", "120"))


# =============================
# LOG SCHEMAS
# =============================
DECISION_FIELDS = [
    "time","symbol","decision","reason",
    "entry","stop","tp","bid","ask","atr","qty",
    "realized_today","trades_today","equity","spy","vwap","note",
    "vol_flag",
    "atr_pct","atr_regime",
    "time_bucket",
]

ATTR_FIELDS = [
    "trade_id","symbol","side","qty",
    "entry_time_et","exit_time_et","hold_seconds",
    "entry_price","stop_price","tp_price","exit_price",
    "exit_reason","outcome","r_multiple",
    "mfe_abs","mae_abs","mfe_r","mae_r",
    "entry_spread_usd","entry_spread_pct","entry_quality","exit_quality",
    "atr_pct_entry","atr_regime",
    "time_bucket_entry",
    "session_date","note",
]

SHADOW_FIELDS = [
    "time","symbol",
    "shadow_decision","shadow_reason",
    "blocked_by",
    "entry","stop","tp","qty",
    "bid","ask","atr","atr_pct","atr_regime",
    "spy","vwap","vol_flag",
    "time_bucket",
    "note",
]


# =============================
# SESSION METRICS
# =============================
DECISION_COUNTS = Counter()
REASON_COUNTS = Counter()
ATR_REGIME_COUNTS = Counter()

SESSION = {
    "trades_submitted": 0,
    "equity_start": None,
    "equity_end": None,
    "equity_last_read": None,
    "realized_today": 0.0,
    "session_date": None,
    "first30_vol_flag": None,  # HIGH/NORMAL/None
    "universe_file": None,
    "symbol_cap": None,
    "run_id": None,
}


# =============================
# HELPERS
# =============================
def now_et() -> datetime:
    return datetime.now(ET)

def safe_float(x, default=0.0) -> float:
    try:
        if x is None:
            return float(default)
        return float(x)
    except Exception:
        return float(default)

# =============================
# RUN IDENTITY + RETRIES
# =============================

def file_sha256_short(path: Path, n: int = 16) -> str:
    try:
        b = path.read_bytes()
        return hashlib.sha256(b).hexdigest()[:n]
    except Exception:
        return "unknown"


def safe_git_head() -> str:
    """Best-effort git commit hash (no hard dependency)."""
    try:
        out = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=str(BASE_DIR))
        return out.decode("utf-8").strip()
    except Exception:
        return "nogit"


def write_run_manifest(script_path: Path) -> None:
    """Writes an identity file so we can always prove what code ran."""
    manifest = {
        "ts_local": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "file": str(script_path.resolve()),
        "mtime_local": time_mod.strftime("%Y-%m-%d %H:%M:%S", time_mod.localtime(script_path.stat().st_mtime)),
        "sha256_16": file_sha256_short(script_path, 16),
        "git_head": safe_git_head(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "alpaca_paper": bool(ALPACA_PAPER),
        "data_feed": str(DATA_FEED),
        "start_trading_et": str(START_TRADING_ET),
        "stop_new_trades_et": str(STOP_NEW_TRADES_ET),
        "market_open_et": str(MARKET_OPEN_ET),
        "market_close_et": str(MARKET_CLOSE_ET),
        "decision_log": str(DECISION_LOG_PATH),
        "shadow_log": str(SHADOW_LOG_PATH),
        "attribution_log": str(ATTRIBUTION_LOG_PATH),
    }
    try:
        RUN_MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    except Exception:
        pass


def banner_startup(script_path: Path) -> None:
    """Console banner (no trade logic change)."""
    print("\n" + "=" * 72)
    print("\U0001F9E0  MYRAA V2.3 BASELINE — PAPER RUN START")
    print(f"FILE   : {script_path.resolve()}")
    print(f"SHA256 : {file_sha256_short(script_path, 16)}   GIT: {safe_git_head()}")
    print(f"FEED   : {DATA_FEED}   PAPER={ALPACA_PAPER}")
    print(f"WINDOW : read-only warmup until {WARMUP_END_ET} | entries {START_TRADING_ET}–{STOP_NEW_TRADES_ET}")
    print(f"LOGS   : {DECISION_LOG_PATH.name} | {SHADOW_LOG_PATH.name} | {ATTRIBUTION_LOG_PATH.name}")
    print("=" * 72 + "\n")


def retry_call(fn, label: str = "call"):
    """Retry wrapper to avoid transient data errors killing the run."""
    last = None
    for i in range(RETRY_ATTEMPTS):
        try:
            return fn()
        except Exception as e:
            last = e
            time_mod.sleep(RETRY_SLEEP_SECONDS * (i + 1))
    raise last


def bars_stale(last_bar_ts_utc: datetime) -> bool:
    """True if the latest bar is too old (data feed lag)."""
    try:
        age = (datetime.now(timezone.utc) - last_bar_ts_utc).total_seconds()
        return age > MAX_BAR_STALENESS_SECONDS
    except Exception:
        return False


def diag_spy_bars(spy_df: pd.DataFrame) -> str:
    """Compact diagnostic string for why SPY bars might be 'not ready'."""
    try:
        if spy_df is None or len(spy_df) == 0:
            return "spy_df_empty"
        if isinstance(spy_df.index, pd.MultiIndex):
            try:
                s = spy_df.xs("SPY").sort_index()
            except Exception:
                return "spy_df_multiindex_missing_spy"
        else:
            s = spy_df.sort_index()
        last_ts = s.index.max()
        return f"spy_bars={len(s)} last_ts={last_ts}"
    except Exception as e:
        return f"spy_diag_error={type(e).__name__}"


def init_csv(path: Path, fields: List[str]) -> None:
    if not path.exists():
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()

def time_bucket_label(dt_et: datetime) -> str:
    t = dt_et.time()
    if t < MARKET_OPEN_ET:
        return "PREMARKET"
    if MARKET_OPEN_ET <= t < time(10, 0):
        return "OPENING_30"
    if time(10, 0) <= t < time(11, 30):
        return "MORNING"
    if time(11, 30) <= t < time(14, 0):
        return "MIDDAY"
    if time(14, 0) <= t < time(15, 0):
        return "AFTERNOON"
    if time(15, 0) <= t <= MARKET_CLOSE_ET:
        return "POWER_HOUR"
    return "AFTER_CLOSE"

def ensure_utc_index(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return df
    if getattr(df.index, "tz", None) is None:
        df = df.copy()
        df.index = df.index.tz_localize(timezone.utc)
    return df

def regular_hours_only(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return df
    df = ensure_utc_index(df)
    idx_et = df.index.tz_convert(ET)
    t = idx_et.time
    mask = (t >= MARKET_OPEN_ET) & (t <= MARKET_CLOSE_ET)
    return df.loc[mask]

def load_universe() -> list[str]:
    syms: list[str] = []

    if MYRAA_SYMBOLS_FILE:
        p = Path(MYRAA_SYMBOLS_FILE)
        if not p.is_absolute():
            p = BASE_DIR / MYRAA_SYMBOLS_FILE
        if p.exists():
            raw = p.read_text(encoding="utf-8", errors="ignore")
            tokens: list[str] = []
            for line in raw.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                tokens.extend([t.strip() for t in line.replace(",", " ").split()])
            syms = tokens

    if not syms:
        syms = [s.strip() for s in MYRAA_SYMBOLS.split(",")]

    syms = [s.upper() for s in syms if s and s.strip()]

    seen = set()
    out: list[str] = []
    for s in syms:
        if s not in seen:
            out.append(s)
            seen.add(s)

    if SYMBOL_CAP > 0:
        out = out[:SYMBOL_CAP]
    return out

def calc_vwap(df: pd.DataFrame) -> float:
    df = regular_hours_only(df)
    if df is None or len(df) == 0:
        return 0.0
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    pv = tp * df["volume"].astype(float)
    denom = df["volume"].astype(float).cumsum().replace(0, pd.NA)
    vwap_series = pv.cumsum() / denom
    return float(vwap_series.iloc[-1])

def calc_atr(df: pd.DataFrame, period: int) -> float:
    df = regular_hours_only(df)
    if df is None or len(df) < max(5, period + 1):
        return 0.0
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift(1)).abs()
    low_close = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr_series = tr.rolling(period).mean()
    v = float(atr_series.iloc[-1])
    return 0.0 if pd.isna(v) else v

def avg_dollar_volume(df: pd.DataFrame) -> float:
    df = regular_hours_only(df)
    if df is None or len(df) == 0:
        return 0.0
    dv = (df["close"].astype(float) * df["volume"].astype(float)).mean()
    return float(dv)

def alpaca_realized_pl(acct) -> float:
    trp = getattr(acct, "today_realized_pl", None)
    if trp is not None:
        return safe_float(trp, 0.0)

    eq = safe_float(getattr(acct, "equity", 0.0), 0.0)
    last_eq = safe_float(getattr(acct, "last_equity", 0.0), 0.0)
    if last_eq > 0:
        return eq - last_eq
    return 0.0

def entry_quality_tag(spread_usd: float, spread_pct: float) -> str:
    if spread_usd <= 0.02 and spread_pct <= 0.00015:
        return "A"
    if spread_usd <= 0.05 and spread_pct <= 0.00030:
        return "B"
    if spread_usd <= 0.10 and spread_pct <= 0.00050:
        return "C"
    return "D"

def exit_quality_tag(exit_reason: str, r_multiple: float) -> str:
    if exit_reason == "TP" and r_multiple >= 1.0:
        return "GOOD"
    if exit_reason == "SL" and r_multiple <= -0.8:
        return "BAD"
    return "UNKNOWN"

def classify_atr_regime(atr_val: float, price: float) -> Tuple[float, str]:
    if price <= 0:
        return 0.0, "UNKNOWN"
    atr_pct = atr_val / price
    if atr_pct < ATR_PCT_LOW:
        return atr_pct, "LOW"
    if atr_pct > ATR_PCT_HIGH:
        return atr_pct, "HIGH"
    return atr_pct, "NORMAL"

def log_decision(sym: str, decision: str, reason: str, **details) -> None:
    ts = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S")
    record = {k: None for k in DECISION_FIELDS}
    record.update({"time": ts, "symbol": sym, "decision": decision, "reason": reason})

    # default tags
    record["time_bucket"] = time_bucket_label(now_et())
    if "vol_flag" not in details:
        record["vol_flag"] = SESSION.get("first30_vol_flag") or "NORMAL"

    record.update(details)

    DECISION_COUNTS[decision] += 1
    REASON_COUNTS[reason] += 1
    if record.get("atr_regime"):
        ATR_REGIME_COUNTS[str(record["atr_regime"])] += 1

    print("🧠", decision, sym, reason)

    with open(DECISION_LOG_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=DECISION_FIELDS)
        w.writerow(record)

def log_shadow(symbol: str, shadow_decision: str, shadow_reason: str, blocked_by: str, **details) -> None:
    ts = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S")
    row = {k: "" for k in SHADOW_FIELDS}
    row.update({
        "time": ts,
        "symbol": symbol,
        "shadow_decision": shadow_decision,
        "shadow_reason": shadow_reason,
        "blocked_by": blocked_by,
        "time_bucket": time_bucket_label(now_et()),
        "vol_flag": SESSION.get("first30_vol_flag") or "NORMAL",
    })
    row.update(details)
    with open(SHADOW_LOG_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SHADOW_FIELDS)
        w.writerow(row)


def _pick_best_candidate_snapshot(data: StockHistoricalDataClient, symbols: List[str], vol_flag: str, spy_last: float, spy_vwap: float) -> dict:
    """
    Tag-only helper:
    Finds the best *market-data* candidate right now (ignores system-level blocks like cutoff/loss/max-trades).
    Uses the same basic filters as the main loop:
      - good quote
      - spread limits (USD + pct) (vol_flag aware)
      - enough RTH bars
      - liquidity (avg dollar vol)
      - ATR + stop calc
      - spread/ATR and spread/risk ratio limits
    Returns dict with best candidate details or {ok:False, reason:"..."}.
    """
    if not symbols:
        return {"ok": False, "reason": "empty_universe"}

    # spread limits depend on vol_flag
    if vol_flag == "HIGH":
        max_spread_usd = MAX_SPREAD_USD_HIGH
        max_spread_pct = MAX_SPREAD_PCT_HIGH
    else:
        max_spread_usd = MAX_SPREAD_USD
        max_spread_pct = MAX_SPREAD_PCT

    # quotes
    try:
        quotes_req = StockLatestQuoteRequest(symbol_or_symbols=symbols, feed=DATA_FEED)
        quotes = data.get_stock_latest_quote(quotes_req)
    except Exception as e:
        return {"ok": False, "reason": f"quote_fetch_failed:{e}"}

    # bars (batch)
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=LIQUIDITY_LOOKBACK_MIN)

    try:
        bars_req = StockBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=TimeFrame.Minute,
            start=start,
            end=end,
            feed=DATA_FEED,
            adjustment="raw",
        )
        bars_df_all = data.get_stock_bars(bars_req).df
    except Exception as e:
        return {"ok": False, "reason": f"bars_fetch_failed:{e}"}

    if bars_df_all is None or len(bars_df_all) == 0:
        return {"ok": False, "reason": "no_bars"}

    best = None  # tuple(score, payload)

    for sym in symbols:
        q = quotes.get(sym)
        if not q:
            continue

        bid = safe_float(getattr(q, "bid_price", 0.0), 0.0)
        ask = safe_float(getattr(q, "ask_price", 0.0), 0.0)
        if bid <= 0 or ask <= 0 or ask < bid:
            continue

        entry_price = float(ask)
        spread = ask - bid
        spread_pct = (spread / entry_price) if entry_price > 0 else 0.0

        # same spread gates as main loop
        if spread < MIN_SPREAD_BID_USD:
            continue
        if spread > max_spread_usd:
            continue
        if spread_pct > max_spread_pct:
            continue

        # slice bars
        if isinstance(bars_df_all.index, pd.MultiIndex):
            if sym not in bars_df_all.index.get_level_values(0):
                continue
            df = bars_df_all.xs(sym).sort_index()
        else:
            df = bars_df_all.sort_index()

        df_rth = regular_hours_only(df)
        if df_rth is None or len(df_rth) < MIN_BARS_REQUIRED:
            continue

        adv = avg_dollar_volume(df_rth)
        if adv < MIN_AVG_DOLLAR_VOL:
            continue

        atr_val = calc_atr(df_rth, period=ATR_PERIOD)
        if atr_val <= 0 or pd.isna(atr_val):
            continue

        atr_pct, atr_regime = classify_atr_regime(atr_val, entry_price)

        # ratios (same as main)
        spread_atr_ratio = (spread / atr_val) if atr_val > 0 else 999.0

        risk_distance = max(STOP_ATR_MULT * atr_val, STOP_FLOOR_USD) + STOP_BUFFER_USD
        stop_price = round(entry_price - risk_distance, 2)
        if stop_price > round(entry_price - 0.01, 2):
            stop_price = round(entry_price - 0.02, 2)

        stop_distance = entry_price - stop_price
        spread_risk_ratio = (spread / stop_distance) if stop_distance > 0 else 999.0

        if spread_atr_ratio > MAX_SPREAD_ATR_RATIO:
            continue
        if spread_risk_ratio > MAX_SPREAD_RISK_RATIO:
            continue
        if stop_price <= 0 or stop_distance <= 0:
            continue

        tp_price = round(entry_price + (TP_R_MULT * stop_distance), 2)
        if tp_price <= round(entry_price + 0.01, 2):
            tp_price = round(entry_price + 0.02, 2)

        qty = int(RISK_PER_TRADE_USD // stop_distance)
        if qty < MIN_QTY:
            qty = MIN_QTY
        if qty > MAX_QTY:
            qty = MAX_QTY

        # score: prioritize tight execution + good risk mechanics (lower is better)
        # (no behavior change; purely "best candidate" tagging)
        score = (spread_risk_ratio * 10.0) + (spread_pct * 10000.0) + (spread_atr_ratio * 5.0)

        payload = {
            "ok": True,
            "symbol": sym,
            "entry": entry_price,
            "stop": stop_price,
            "tp": tp_price,
            "qty": qty,
            "bid": bid,
            "ask": ask,
            "atr": atr_val,
            "atr_pct": atr_pct,
            "atr_regime": atr_regime,
            "spy": spy_last,
            "vwap": spy_vwap,
            "vol_flag": vol_flag,
            "note": f"best_candidate score={score:.4f} spread={spread:.4f} spread_pct={spread_pct:.6f} spread_atr_ratio={spread_atr_ratio:.4f} spread_risk_ratio={spread_risk_ratio:.4f} adv={adv:.0f}",
        }

        if best is None or score < best[0]:
            best = (score, payload)

    if best is None:
        return {"ok": False, "reason": "no_candidate_passed_filters"}

    return best[1]


def log_blocked_by(block_reason: str, data: StockHistoricalDataClient, symbols: List[str],
                   spy_last: float = 0.0, spy_vwap: float = 0.0,
                   equity: float = 0.0, realized_today: float = 0.0, trades_today: int = 0,
                   vol_flag: str = "NORMAL", note: str = "") -> None:
    """
    Writes exactly ONE line to shadow_log_v2.csv describing:
      - what blocked entries (block_reason)
      - the best candidate at that moment (if any)
    Tag-only: does not affect behavior.
    """
    snap = _pick_best_candidate_snapshot(data, symbols, vol_flag=vol_flag, spy_last=spy_last, spy_vwap=spy_vwap)

    if snap.get("ok"):
        log_shadow(
            snap["symbol"],
            shadow_decision="BLOCKED_BY",
            shadow_reason=block_reason,
            blocked_by=block_reason,
            entry=snap.get("entry"),
            stop=snap.get("stop"),
            tp=snap.get("tp"),
            qty=snap.get("qty"),
            bid=snap.get("bid"),
            ask=snap.get("ask"),
            atr=snap.get("atr"),
            atr_pct=snap.get("atr_pct"),
            atr_regime=snap.get("atr_regime"),
            spy=snap.get("spy"),
            vwap=snap.get("vwap"),
            vol_flag=snap.get("vol_flag"),
            time_bucket=time_bucket_label(now_et()),
            note=("; ".join([x for x in [note, snap.get("note",""), f"equity={equity:.2f}", f"realized_today={realized_today:.2f}", f"trades_today={trades_today}"] if x])).strip(),
        )
    else:
        # no candidate — still write one line (symbol=SYSTEM)
        log_shadow(
            "SYSTEM",
            shadow_decision="BLOCKED_BY",
            shadow_reason=block_reason,
            blocked_by=block_reason,
            spy=spy_last,
            vwap=spy_vwap,
            vol_flag=vol_flag,
            time_bucket=time_bucket_label(now_et()),
            note=("; ".join([x for x in [note, f"no_candidate={snap.get('reason')}", f"equity={equity:.2f}", f"realized_today={realized_today:.2f}", f"trades_today={trades_today}"] if x])).strip(),
        )

def compute_first30_vol_flag(data: StockHistoricalDataClient, session_date: date_cls) -> Optional[str]:
    now_local = now_et()
    if now_local.time() < FIRST30_LOCK_TIME_ET:
        return None

    start_local = datetime.combine(session_date, time(9, 30), tzinfo=ET)
    end_local = datetime.combine(session_date, time(10, 0), tzinfo=ET)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)

    spy_req = StockBarsRequest(
        symbol_or_symbols="SPY",
        timeframe=TimeFrame.Minute,
        start=start_utc,
        end=end_utc,
        feed=DATA_FEED,
        adjustment="raw",
    )
    df = data.get_stock_bars(spy_req).df
    if df is None or len(df) < 10:
        return "NORMAL"

    if isinstance(df.index, pd.MultiIndex):
        df = df.xs("SPY").sort_index()
    else:
        df = df.sort_index()

    closes = df["close"].astype(float)
    rets = closes.pct_change().dropna()
    realized = float(rets.std()) if len(rets) else 0.0

    high = float(df["high"].max())
    low = float(df["low"].min())
    last = float(closes.iloc[-1]) if len(closes) else 0.0
    range_pct = ((high - low) / last) if last > 0 else 0.0

    is_high = (realized > FIRST30_REALIZED_VOL_TH) or (range_pct > FIRST30_RANGE_PCT_TH)
    return "HIGH" if is_high else "NORMAL"


def write_daily_report(session_date: str) -> str:
    report_path = REPORTS_DIR / f"daily_report_{session_date}_{now_et().strftime('%H%M%S')}.txt"
    latest_path = REPORTS_DIR / "daily_report_latest.txt"

    eq_start = safe_float(SESSION.get("equity_start", 0.0), 0.0)
    eq_end = safe_float(SESSION.get("equity_end", 0.0), 0.0)
    eq_change = eq_end - eq_start

    top_reasons = REASON_COUNTS.most_common(12)

    lines: list[str] = []
    lines.append(f"MYRAA Daily Report — {session_date}")
    lines.append("=" * 48)
    lines.append(f"Run id: {SESSION.get('run_id')}")
    lines.append(f"Trades submitted: {int(SESSION.get('trades_submitted', 0) or 0)}")
    lines.append(f"Realized P&L (today): {safe_float(SESSION.get('realized_today', 0.0), 0.0):.2f}")
    lines.append("")
    lines.append(f"Equity (start): {eq_start:.2f}")
    lines.append(f"Equity (end):   {eq_end:.2f}")
    lines.append(f"Equity change:  {eq_change:.2f}")
    lines.append("")
    lines.append(f"First30 vol flag: {SESSION.get('first30_vol_flag')}")
    lines.append(f"Universe file: {SESSION.get('universe_file')}  cap={SESSION.get('symbol_cap')}")
    lines.append(f"Decision log: {DECISION_LOG_PATH.name}")
    lines.append(f"Attribution:  {ATTRIBUTION_LOG_PATH.name}")
    lines.append(f"Shadow log:   {SHADOW_LOG_PATH.name}")
    lines.append("")
    lines.append(f"Spread limits NORMAL: MAX_SPREAD_USD={MAX_SPREAD_USD} MAX_SPREAD_PCT={MAX_SPREAD_PCT}")
    lines.append(f"Spread limits HIGH:   MAX_SPREAD_USD_HIGH={MAX_SPREAD_USD_HIGH} MAX_SPREAD_PCT_HIGH={MAX_SPREAD_PCT_HIGH}")
    lines.append(f"Spread ratios: MAX_SPREAD_ATR_RATIO={MAX_SPREAD_ATR_RATIO} MAX_SPREAD_RISK_RATIO={MAX_SPREAD_RISK_RATIO}")
    lines.append(f"ATR regime thresholds: ATR_PCT_LOW={ATR_PCT_LOW} ATR_PCT_HIGH={ATR_PCT_HIGH}")
    lines.append("")
    lines.append(f"Decision counts: {dict(DECISION_COUNTS)}")
    lines.append("")
    lines.append("ATR regime tag counts (tag-only):")
    if ATR_REGIME_COUNTS:
        for k, v in ATR_REGIME_COUNTS.most_common():
            lines.append(f"  {v:>6}  {k}")
    else:
        lines.append("  (none)")
    lines.append("")
    lines.append("Top reasons (histogram):")
    if top_reasons:
        for reason, count in top_reasons:
            lines.append(f"  {count:>6}  {reason}")
        top_reason = top_reasons[0][0]
    else:
        lines.append("  (no reasons recorded)")
        top_reason = "none"

    text = "\n".join(lines) + "\n"
    report_path.write_text(text, encoding="utf-8")
    latest_path.write_text(text, encoding="utf-8")
    print("\n" + text)
    return top_reason


# =============================
# TRADE TRACKING (MFE/MAE + ATTRIBUTION)
# =============================
@dataclass
class TrackedTrade:
    trade_id: str
    symbol: str
    side: str
    qty: int
    entry_time: datetime
    entry_price: float
    stop_price: float
    tp_price: float
    entry_spread_usd: float
    entry_spread_pct: float
    entry_quality: str
    atr_pct_entry: float
    atr_regime: str
    time_bucket_entry: str
    max_price_seen: float = 0.0
    min_price_seen: float = 0.0
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: str = "Unknown"
    outcome: str = "Unknown"
    exit_quality: str = "UNKNOWN"

    def risk_distance(self) -> float:
        return max(self.entry_price - self.stop_price, 1e-9)

    def r_multiple(self) -> float:
        if self.exit_price is None:
            return 0.0
        return (self.exit_price - self.entry_price) / self.risk_distance()

    def mfe_abs(self) -> float:
        if self.max_price_seen <= 0:
            return 0.0
        return max(0.0, self.max_price_seen - self.entry_price)

    def mae_abs(self) -> float:
        if self.min_price_seen <= 0:
            return 0.0
        return max(0.0, self.entry_price - self.min_price_seen)

    def mfe_r(self) -> float:
        return self.mfe_abs() / self.risk_distance()

    def mae_r(self) -> float:
        return self.mae_abs() / self.risk_distance()

    def hold_seconds(self) -> int:
        if not self.exit_time:
            return 0
        return int((self.exit_time - self.entry_time).total_seconds())


class TradeTracker:
    def __init__(self, attribution_path: Path):
        self.attribution_path = attribution_path
        self.open_trades: Dict[str, TrackedTrade] = {}

    def has_open_trades(self) -> bool:
        return len(self.open_trades) > 0

    def register_trade(
        self,
        symbol: str,
        qty: int,
        entry_price: float,
        stop_price: float,
        tp_price: float,
        spread_usd: float,
        spread_pct: float,
        atr_pct_entry: float,
        atr_regime: str,
        time_bucket_entry: str,
        seq: int,
    ) -> TrackedTrade:
        trade_id = f"{now_et().strftime('%Y%m%d_%H%M%S')}_{symbol}_{seq}"
        eq = entry_quality_tag(spread_usd, spread_pct)

        tr = TrackedTrade(
            trade_id=trade_id,
            symbol=symbol,
            side="BUY",
            qty=int(qty),
            entry_time=now_et(),
            entry_price=float(entry_price),
            stop_price=float(stop_price),
            tp_price=float(tp_price),
            entry_spread_usd=float(spread_usd),
            entry_spread_pct=float(spread_pct),
            entry_quality=eq,
            atr_pct_entry=float(atr_pct_entry),
            atr_regime=str(atr_regime),
            time_bucket_entry=str(time_bucket_entry),
            max_price_seen=float(entry_price),
            min_price_seen=float(entry_price),
        )
        self.open_trades[symbol] = tr
        return tr

    def update_live_marks(self, data: StockHistoricalDataClient, symbols: List[str]) -> None:
        if not symbols:
            return
        try:
            req = StockLatestTradeRequest(symbol_or_symbols=symbols, feed=DATA_FEED)
            latest = data.get_stock_latest_trade(req)
        except Exception:
            return

        for sym in symbols:
            tr = self.open_trades.get(sym)
            if not tr:
                continue
            t = latest.get(sym)
            px = safe_float(getattr(t, "price", None), 0.0)
            if px <= 0:
                continue
            if px > tr.max_price_seen:
                tr.max_price_seen = px
            if px < tr.min_price_seen:
                tr.min_price_seen = px

    def finalize_exits(self, trading: TradingClient, session_date: str) -> None:
        """Reconcile exits with broker fills and derive consistent exit labeling."""

        def _order_attr(o, name, default=None):
            try:
                if isinstance(o, dict):
                    return o.get(name, default)
            except Exception:
                pass
            return getattr(o, name, default)

        def _as_float(x):
            try:
                return float(x)
            except Exception:
                return None

        def _best_exit_order(trade):
            want_side = "sell" if trade.side.upper() == "BUY" else "buy"
            entry_ts = trade.entry_time
            candidates = []

            for o in closed_orders:
                try:
                    if _order_attr(o, "symbol") != trade.symbol:
                        continue
                    if str(_order_attr(o, "side", "")).lower() != want_side:
                        continue
                    filled_at = _order_attr(o, "filled_at")
                    if not filled_at:
                        continue
                    try:
                        filled_at_et = filled_at.astimezone(ET)
                    except Exception:
                        filled_at_et = filled_at
                    if filled_at_et < (entry_ts - timedelta(seconds=5)):
                        continue
                    fq = _as_float(_order_attr(o, "filled_qty"))
                    if fq is not None and abs(fq - float(trade.qty)) > 1e-6:
                        continue
                    candidates.append((filled_at_et, o))
                except Exception:
                    continue

            if not candidates:
                return None
            candidates.sort(key=lambda t: t[0])
            return candidates[0]

        try:
            closed_orders = trading.get_orders(status="closed", limit=500)
        except Exception:
            closed_orders = []

        to_close = list(self.open_trades.keys())

        for sym in to_close:
            tr = self.open_trades.get(sym)
            if not tr:
                continue

            tr.exit_time = tr.exit_time or now_et()
            tr.exit_price = tr.exit_price or tr.entry_price
            exit_order = None

            found = _best_exit_order(tr)
            if found:
                exit_time_et, o = found
                exit_order = o
                tr.exit_time = exit_time_et
                px = None
                for fld in ("filled_avg_price", "avg_fill_price"):
                    if px is None:
                        px = _as_float(_order_attr(o, fld))
                if px is None:
                    px = _as_float(_order_attr(o, "limit_price"))
                if px is None:
                    px = _as_float(_order_attr(o, "stop_price"))
                if px is not None:
                    tr.exit_price = px

            if tr.exit_price >= tr.tp_price:
                tr.exit_reason = "TP"
                tr.outcome = "WIN"
            elif tr.exit_price <= tr.stop_price:
                tr.exit_reason = "SL"
                tr.outcome = "LOSS"
            else:
                tr.exit_reason = tr.exit_reason or "EXIT"
                tr.outcome = tr.outcome or "FLAT"

            tr.exit_quality = exit_quality_tag(tr.exit_reason, tr.r_multiple())
            self._write_attribution_row(tr, session_date=session_date)
            del self.open_trades[sym]

    def _write_attribution_row(self, tr: TrackedTrade, session_date: str) -> None:
        row = {k: "" for k in ATTR_FIELDS}
        row.update({
            "trade_id": tr.trade_id,
            "symbol": tr.symbol,
            "side": tr.side,
            "qty": tr.qty,
            "entry_time_et": tr.entry_time.strftime("%Y-%m-%d %H:%M:%S"),
            "exit_time_et": tr.exit_time.strftime("%Y-%m-%d %H:%M:%S") if tr.exit_time else "",
            "hold_seconds": tr.hold_seconds(),
            "entry_price": round(tr.entry_price, 4),
            "stop_price": round(tr.stop_price, 4),
            "tp_price": round(tr.tp_price, 4),
            "exit_price": round(tr.exit_price, 4) if tr.exit_price is not None else "",
            "exit_reason": tr.exit_reason,
            "outcome": tr.outcome,
            "r_multiple": round(tr.r_multiple(), 4),
            "mfe_abs": round(tr.mfe_abs(), 4),
            "mae_abs": round(tr.mae_abs(), 4),
            "mfe_r": round(tr.mfe_r(), 4),
            "mae_r": round(tr.mae_r(), 4),
            "entry_spread_usd": round(tr.entry_spread_usd, 6),
            "entry_spread_pct": f"{tr.entry_spread_pct:.8f}",
            "entry_quality": tr.entry_quality,
            "exit_quality": tr.exit_quality,
            "atr_pct_entry": f"{tr.atr_pct_entry:.8f}",
            "atr_regime": tr.atr_regime,
            "time_bucket_entry": tr.time_bucket_entry,
            "session_date": session_date,
            "note": "",
        })

        with open(self.attribution_path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=ATTR_FIELDS)
            w.writerow(row)


# =============================
# STARTUP BANNER (console only)
# =============================
def startup_banner(symbols: List[str], acct_equity: str) -> None:
    rid = SESSION.get("run_id")
    print("")
    print("🧠 MYRAA V2.1 — PAPER EXECUTION + LOGGING")
    print("==================================================")
    print(f"Run ID: {rid}")
    print(f"Paper:  {ALPACA_PAPER}   Feed: {DATA_FEED}")
    print(f"Equity: {acct_equity}")
    print(f"Universe: {len(symbols)} symbols  cap={SYMBOL_CAP}")
    print(f"Start: {START_TRADING_ET}   Cutoff: {STOP_NEW_TRADES_ET}   Cooldown: {COOLDOWN_MINUTES}m")
    print(f"Market-hours-only stats: ON ({MARKET_OPEN_ET}-{MARKET_CLOSE_ET} ET)")
    print(f"Logs: {DECISION_LOG_PATH.name} | {ATTRIBUTION_LOG_PATH.name} | {SHADOW_LOG_PATH.name}")
    print("==================================================")
    print("")


# =============================
# MAIN
# =============================
def main() -> None:
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY in .env")

    SESSION["run_id"] = now_et().strftime("%H%M%S")

    init_csv(DECISION_LOG_PATH, DECISION_FIELDS)
    init_csv(ATTRIBUTION_LOG_PATH, ATTR_FIELDS)
    init_csv(SHADOW_LOG_PATH, SHADOW_FIELDS)

    script_path = Path(__file__)
    write_run_manifest(script_path)
    banner_startup(script_path)

    trading = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=ALPACA_PAPER)
    data = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

    symbols = load_universe()
    if not symbols:
        raise RuntimeError("Universe is empty. Set MYRAA_SYMBOLS or MYRAA_SYMBOLS_FILE.")

    SESSION["universe_file"] = MYRAA_SYMBOLS_FILE or "(env:MYRAA_SYMBOLS)"
    SESSION["symbol_cap"] = SYMBOL_CAP
    SESSION["session_date"] = str(now_et().date())

    acct = trading.get_account()
    equity_start = safe_float(getattr(acct, "equity", 0.0), 0.0)
    SESSION["equity_start"] = equity_start
    SESSION["equity_last_read"] = equity_start

    startup_banner(symbols, str(getattr(acct, "equity", None)))

    traded_this_session = set()
    last_trade_time_et: Dict[str, datetime] = {}
    cooldown = timedelta(minutes=COOLDOWN_MINUTES)
    session_date = now_et().date()
    trades_today = 0

    last_spy_log_ts: Optional[float] = None

    tracker = TradeTracker(ATTRIBUTION_LOG_PATH)
    trade_seq = 0

    try:
        while True:

            # Heartbeat (proves process is alive; no trade logic change)
            if "last_heartbeat_ts" not in SESSION:
                SESSION["last_heartbeat_ts"] = 0.0
            if (time_mod.time() - float(SESSION["last_heartbeat_ts"])) >= HEARTBEAT_SECONDS:
                SESSION["last_heartbeat_ts"] = time_mod.time()
                log_decision("SYSTEM", "INFO", "heartbeat", equity=SESSION.get("equity_last_read", ""), realized_today=SESSION.get("realized_today", ""), trades_today=SESSION.get("trades_submitted", 0), vol_flag=(SESSION.get("first30_vol_flag") or "NORMAL"))
            # --- update tracking ---
            if tracker.has_open_trades():
                tracker.update_live_marks(data, list(tracker.open_trades.keys()))
                tracker.finalize_exits(trading, session_date=str(session_date))

            # market open?
            clock = trading.get_clock()
            if not clock.is_open:
                log_decision("SYSTEM", "SKIP", "market_closed", note="waiting_market_open")
                time_mod.sleep(30)
                continue

            now_t = now_et().time()

            # Trading is disabled during warmup (read-only) and after cutoff, but we still
            # run the full data + filters stack so logs remain counterfactual-ready.
            allow_new_entries = (now_t >= START_TRADING_ET) and (now_t < STOP_NEW_TRADES_ET)
            trading_disabled = not allow_new_entries

            # account snapshot
            acct = trading.get_account()
            equity_now = safe_float(getattr(acct, "equity", None), SESSION["equity_last_read"] or 0.0)
            SESSION["equity_last_read"] = equity_now

            realized_today = alpaca_realized_pl(acct)
            SESSION["realized_today"] = realized_today

            if realized_today <= (-1.0 * DAILY_LOSS_LIMIT_USD):
                log_decision("SYSTEM", "SKIP", "daily_loss_limit_hit", realized_today=realized_today, equity=equity_now)
                log_blocked_by("daily_loss_limit_hit", data, symbols, spy_last=0.0, spy_vwap=0.0, equity=equity_now, realized_today=realized_today, trades_today=trades_today, vol_flag=(SESSION.get("first30_vol_flag") or "NORMAL"), note="system_block")
                time_mod.sleep(120)
                continue

            if trades_today >= MAX_TRADES_PER_DAY:
                log_decision("SYSTEM", "SKIP", "max_trades_reached", trades_today=trades_today, equity=equity_now)
                log_blocked_by("max_trades_reached", data, symbols, spy_last=0.0, spy_vwap=0.0, equity=equity_now, realized_today=realized_today, trades_today=trades_today, vol_flag=(SESSION.get("first30_vol_flag") or "NORMAL"), note="system_block")
                time_mod.sleep(60)
                continue

            # In V2.3 we keep processing during warmup / after-cutoff (read-only mode)
            # so filters, candidate scoring, and shadow logs still populate.
            trading_disabled = False
            if not allow_new_entries:
                trading_disabled = True
                block_reason = "warmup_read_only" if now_t < START_TRADING_ET else "after_cutoff"
                note = "warming_up_no_trades" if block_reason == "warmup_read_only" else "no_new_entries_after_cutoff"
                log_decision("SYSTEM", "SKIP", block_reason, note=note, equity=equity_now)
                log_blocked_by(block_reason, data, symbols, spy_last=0.0, spy_vwap=0.0, equity=equity_now, realized_today=realized_today, trades_today=trades_today, vol_flag=(SESSION.get("first30_vol_flag") or "NORMAL"), note="system_block")

            # lock first30 vol flag once
            if SESSION.get("first30_vol_flag") is None:
                flag = compute_first30_vol_flag(data, session_date)
                if flag is not None:
                    SESSION["first30_vol_flag"] = flag
                    log_decision("SYSTEM", "INFO", "first30_vol_flag_locked", note=f"flag={flag}")

            vol_flag = SESSION.get("first30_vol_flag") or "NORMAL"
            if vol_flag == "HIGH":
                max_spread_usd = MAX_SPREAD_USD_HIGH
                max_spread_pct = MAX_SPREAD_PCT_HIGH
            else:
                max_spread_usd = MAX_SPREAD_USD
                max_spread_pct = MAX_SPREAD_PCT

            # positions / orders
            positions = {p.symbol for p in trading.get_all_positions()}
            open_orders = trading.get_orders()
            open_order_symbols = {
                o.symbol for o in open_orders
                if str(getattr(o, "status", "")).lower() in ("new", "accepted", "pending_new", "partially_filled")
            }

            # quotes
            quotes_req = StockLatestQuoteRequest(symbol_or_symbols=symbols, feed=DATA_FEED)
            quotes = data.get_stock_latest_quote(quotes_req)

            # SPY VWAP filter (market-hours-only calc_vwap)
            end_m = datetime.now(timezone.utc)
            start_m = end_m - timedelta(minutes=SPY_VWAP_LOOKBACK_MIN)
            spy_req = StockBarsRequest(
                symbol_or_symbols="SPY",
                timeframe=TimeFrame.Minute,
                start=start_m,
                end=end_m,
                feed=DATA_FEED,
                adjustment="raw",
            )
            spy_df = data.get_stock_bars(spy_req).df
            if spy_df is None or len(spy_df) < 30:
                log_decision("SPY", "SKIP", "spy_bars_not_ready", note=diag_spy_bars(spy_df))
                time_mod.sleep(20)
                continue

            if isinstance(spy_df.index, pd.MultiIndex):
                spy_df = spy_df.xs("SPY").sort_index()
            else:
                spy_df = spy_df.sort_index()

            spy_last = float(spy_df["close"].iloc[-1])
            spy_vwap = calc_vwap(spy_df)

            if spy_vwap > 0 and spy_last < spy_vwap:
                now_ts = time_mod.time()
                should_log = last_spy_log_ts is None or (now_ts - last_spy_log_ts) >= SPY_THROTTLE_SECONDS
                if should_log:
                    last_spy_log_ts = now_ts
                    log_decision("SPY", "SKIP", "market_filter_spy_below_vwap", spy=spy_last, vwap=spy_vwap, note="throttled_log")
                    log_blocked_by("market_filter_spy_below_vwap", data, symbols, spy_last=spy_last, spy_vwap=spy_vwap, equity=equity_now, realized_today=realized_today, trades_today=trades_today, vol_flag=(SESSION.get("first30_vol_flag") or "NORMAL"), note="spy_filter_block")
                time_mod.sleep(20)
                continue

            # Build candidate list (fast checks)
            candidates: list[str] = []
            for sym in symbols:
                if sym in open_order_symbols:
                    log_decision(sym, "SKIP", "open_order_exists", equity=equity_now)
                    continue
                if sym in positions:
                    log_decision(sym, "SKIP", "already_in_position", equity=equity_now)
                    continue
                if sym in traded_this_session:
                    log_decision(sym, "SKIP", "already_traded_this_session", equity=equity_now)
                    continue

                last_dt = last_trade_time_et.get(sym)
                if last_dt is not None and (now_et() - last_dt) < cooldown:
                    log_decision(sym, "SKIP", "cooldown_active", equity=equity_now)
                    continue

                q = quotes.get(sym)
                if not q:
                    log_decision(sym, "SKIP", "no_quote", equity=equity_now)
                    continue

                bid = safe_float(getattr(q, "bid_price", 0.0), 0.0)
                ask = safe_float(getattr(q, "ask_price", 0.0), 0.0)
                if bid <= 0 or ask <= 0 or ask < bid:
                    log_decision(sym, "SKIP", "bad_quote", bid=bid, ask=ask, equity=equity_now)
                    continue

                entry_price = float(ask)

                spread = ask - bid
                spread_pct = (spread / entry_price) if entry_price > 0 else 0.0

                if spread < MIN_SPREAD_BID_USD:
                    log_decision(sym, "SKIP", "spread_too_small", bid=bid, ask=ask, equity=equity_now, note=f"spread={spread:.4f}")
                    continue
                if spread > max_spread_usd:
                    log_decision(sym, "SKIP", "spread_too_wide_usd", bid=bid, ask=ask, equity=equity_now, note=f"spread={spread:.4f}")
                    continue
                if spread_pct > max_spread_pct:
                    log_decision(sym, "SKIP", "spread_too_wide_pct", bid=bid, ask=ask, equity=equity_now, note=f"spread_pct={spread_pct:.6f}")
                    continue

                candidates.append(sym)

            if not candidates:
                time_mod.sleep(LOOP_SLEEP_SECONDS)
                continue

            # Batch bars (we will apply market-hours-only inside stats funcs)
            end = datetime.now(timezone.utc)
            start = end - timedelta(minutes=LIQUIDITY_LOOKBACK_MIN)

            bars_req = StockBarsRequest(
                symbol_or_symbols=candidates,
                timeframe=TimeFrame.Minute,
                start=start,
                end=end,
                feed=DATA_FEED,
                adjustment="raw",
            )
            bars_df_all = data.get_stock_bars(bars_req).df
            if bars_df_all is None or len(bars_df_all) == 0:
                for sym in candidates:
                    log_decision(sym, "SKIP", "not_enough_bars", equity=equity_now)
                time_mod.sleep(LOOP_SLEEP_SECONDS)
                continue

            # Evaluate each candidate
            for sym in candidates:
                if trades_today >= MAX_TRADES_PER_DAY:
                    break

                q = quotes.get(sym)
                bid = safe_float(getattr(q, "bid_price", 0.0), 0.0)
                ask = safe_float(getattr(q, "ask_price", 0.0), 0.0)
                entry_price = float(ask)

                if isinstance(bars_df_all.index, pd.MultiIndex):
                    if sym not in bars_df_all.index.get_level_values(0):
                        log_decision(sym, "SKIP", "not_enough_bars", equity=equity_now)
                        continue
                    df = bars_df_all.xs(sym).sort_index()
                else:
                    df = bars_df_all.sort_index()

                # market-hours-only filtering impacts bar count
                df_rth = regular_hours_only(df)
                if df_rth is None or len(df_rth) < MIN_BARS_REQUIRED:
                    log_decision(sym, "SKIP", "not_enough_bars", equity=equity_now)
                    continue

                adv = avg_dollar_volume(df_rth)
                if adv < MIN_AVG_DOLLAR_VOL:
                    log_decision(sym, "SKIP", "liquidity_too_low", equity=equity_now, note=f"avg_dollar_vol={adv:.0f}")
                    continue

                atr_val = calc_atr(df_rth, period=ATR_PERIOD)
                if atr_val <= 0 or pd.isna(atr_val):
                    log_decision(sym, "SKIP", "atr_invalid", atr=atr_val, equity=equity_now)
                    continue

                atr_pct, atr_regime = classify_atr_regime(atr_val, entry_price)

                spread = ask - bid
                spread_pct = (spread / entry_price) if entry_price > 0 else 0.0
                spread_atr_ratio = (spread / atr_val) if atr_val > 0 else 999.0

                risk_distance = max(STOP_ATR_MULT * atr_val, STOP_FLOOR_USD) + STOP_BUFFER_USD
                stop_price = round(entry_price - risk_distance, 2)
                if stop_price > round(entry_price - 0.01, 2):
                    stop_price = round(entry_price - 0.02, 2)

                stop_distance = entry_price - stop_price
                spread_risk_ratio = (spread / stop_distance) if stop_distance > 0 else 999.0

                if spread_atr_ratio > MAX_SPREAD_ATR_RATIO:
                    log_decision(sym, "SKIP", "spread_to_atr_too_high",
                                 bid=bid, ask=ask, atr=atr_val, atr_pct=atr_pct, atr_regime=atr_regime,
                                 equity=equity_now, note=f"spread_atr_ratio={spread_atr_ratio:.4f}")
                    continue

                if spread_risk_ratio > MAX_SPREAD_RISK_RATIO:
                    log_decision(sym, "SKIP", "spread_to_risk_too_high",
                                 bid=bid, ask=ask, atr=atr_val, atr_pct=atr_pct, atr_regime=atr_regime,
                                 equity=equity_now, note=f"spread_risk_ratio={spread_risk_ratio:.4f}")
                    continue

                if stop_price <= 0 or stop_distance <= 0:
                    log_decision(sym, "SKIP", "stop_distance_invalid",
                                 entry=entry_price, stop=stop_price, atr=atr_val, atr_pct=atr_pct, atr_regime=atr_regime,
                                 equity=equity_now)
                    continue

                tp_price = round(entry_price + (TP_R_MULT * stop_distance), 2)
                if tp_price <= round(entry_price + 0.01, 2):
                    tp_price = round(entry_price + 0.02, 2)

                qty = int(RISK_PER_TRADE_USD // stop_distance)
                if qty < MIN_QTY:
                    qty = MIN_QTY
                if qty > MAX_QTY:
                    qty = MAX_QTY

                # Read-only mode (warmup) or after-cutoff: log what WOULD have been submitted,
                # but do not change behavior by placing orders.
                if trading_disabled:
                    block_reason = "warmup_read_only" if now_t < START_TRADING_ET else "after_cutoff"
                    tb = time_bucket_label(now_et())
                    log_shadow(
                        sym,
                        shadow_type="WOULD_TRADE",
                        blocked_by=block_reason,
                        decision="TRADE",
                        reason="submit_order",
                        entry=entry_price, stop=stop_price, tp=tp_price,
                        bid=bid, ask=ask, atr=atr_val,
                        atr_pct=atr_pct, atr_regime=atr_regime,
                        qty=qty,
                        realized_today=realized_today,
                        trades_today=trades_today,
                        equity=equity_now,
                        spy=spy_last,
                        vwap=spy_vwap,
                        vol_flag=vol_flag,
                        time_bucket=tb,
                        note="trade_blocked_read_only",
                    )
                    log_decision(sym, "SKIP", block_reason, equity=equity_now, vol_flag=vol_flag, note="trade_blocked_read_only")
                    continue

                order = MarketOrderRequest(
                    symbol=sym,
                    qty=qty,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY,
                    order_class=OrderClass.BRACKET,
                    stop_loss=StopLossRequest(stop_price=stop_price),
                    take_profit=TakeProfitRequest(limit_price=tp_price),
                )

                try:
                    trade_seq += 1
                    tb = time_bucket_label(now_et())
                    log_decision(
                        sym, "TRADE", "submit_order",
                        entry=entry_price, stop=stop_price, tp=tp_price,
                        bid=bid, ask=ask, atr=atr_val,
                        atr_pct=atr_pct, atr_regime=atr_regime,
                        qty=qty,
                        realized_today=realized_today,
                        trades_today=trades_today,
                        equity=equity_now,
                        spy=spy_last,
                        vwap=spy_vwap,
                        vol_flag=vol_flag,
                        time_bucket=tb,
                        note=f"spread={spread:.4f} spread_pct={spread_pct:.6f}",
                    )

                    trading.submit_order(order)

                    tracker.register_trade(
                        symbol=sym,
                        qty=qty,
                        entry_price=entry_price,
                        stop_price=stop_price,
                        tp_price=tp_price,
                        spread_usd=spread,
                        spread_pct=spread_pct,
                        atr_pct_entry=atr_pct,
                        atr_regime=atr_regime,
                        time_bucket_entry=tb,
                        seq=trade_seq,
                    )

                    SESSION["trades_submitted"] = int(SESSION.get("trades_submitted", 0) or 0) + 1

                except Exception as e:
                    log_decision(sym, "ERROR", "order_rejected", note=str(e), equity=equity_now,
                                 atr=atr_val, atr_pct=atr_pct, atr_regime=atr_regime)
                    continue

                traded_this_session.add(sym)
                last_trade_time_et[sym] = now_et()
                trades_today += 1

                time_mod.sleep(2)

            time_mod.sleep(LOOP_SLEEP_SECONDS)

    except KeyboardInterrupt:
        log_decision("SYSTEM", "EXIT", "shutdown", note="KeyboardInterrupt")

    except Exception as e:
        log_decision("SYSTEM", "EXIT", "crash", note=str(e))

    finally:
        try:
            tracker.finalize_exits(trading, session_date=str(now_et().date()))
        except Exception:
            pass

        try:
            acct = trading.get_account()
            SESSION["equity_end"] = safe_float(getattr(acct, "equity", 0.0), 0.0)
            SESSION["realized_today"] = alpaca_realized_pl(acct)
        except Exception:
            SESSION["equity_end"] = safe_float(SESSION.get("equity_start", 0.0), 0.0)

        top_reason = write_daily_report(str(now_et().date()))
        log_decision("SYSTEM", "SUMMARY", "daily_summary",
                     realized_today=SESSION["realized_today"],
                     trades_today=int(SESSION.get("trades_submitted", 0) or 0),
                     equity=SESSION["equity_end"],
                     note=f"top_reason={top_reason}")


if __name__ == "__main__":
    main()
