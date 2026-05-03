
"""
Angel One ORB Backtest — v3 (Filtered)
========================================

Strategy:
    30-min Opening Range Breakout, long-only, fixed 1.5R target,
    OR-low stop, one trade per day, square-off at 15:15 IST.

What's new in v3 — four data-driven filters:
    A) Breakout strength       : require entry candle close >= OR-high * (1 + min_bo_pct/100)
    B) Day-of-week filter      : skip listed days (defaults: Friday)
    C) Volume confirmation     : breakout candle volume >= vol_mult × avg OR volume
    D) Nifty index alignment   : only enter if Nifty 50 is also above its OR-high

Why these filters:
    Empirical analysis of RELIANCE + HDFCBANK over 4 months showed:
      - Breakouts barely above OR-high (<0.1%) had 39% win rate.
      - Breakouts >=0.3% above OR-high had 67% win rate.
      - Friday trades lost heavily; Mon/Wed had positive expectancy.
      - Fakeouts on low volume are common; volume ≥ 1.5x avg helps.
      - Nifty-aligned breakouts tend to follow through; counter-index ones fade.

CLI usage:
    python angel_orb_backtest.py -s RELIANCE
    python angel_orb_backtest.py -s HDFCBANK --days 180 --capital 200000
    python angel_orb_backtest.py -s INFY --min-bo-pct 0.15 --vol-mult 1.5
    python angel_orb_backtest.py -s TCS --skip-days Friday Thursday
    python angel_orb_backtest.py -s ICICIBANK --no-nifty-filter
    python angel_orb_backtest.py -s RELIANCE --no-filters   # baseline (no filters)

Set credentials in a .env file in the same folder:
    ANGEL_API_KEY=...
    ANGEL_CLIENT_ID=...
    ANGEL_PIN=...
    ANGEL_TOTP_SECRET=...
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, time as dtime, timedelta, date
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd
import pyotp
import requests
from dotenv import load_dotenv
from SmartApi import SmartConnect

load_dotenv()


# ---------- Constants -------------------------------------------------------

IST = ZoneInfo("Asia/Kolkata")

OR_END = dtime(9, 45)
NO_NEW_ENTRY_AFTER = dtime(14, 30)
SQUARE_OFF = dtime(15, 15)
TARGET_R_MULTIPLE = 1.5
MIN_OR_WIDTH_PCT = 0.20

SCRIP_MASTER_URL = (
    "https://margincalculator.angelbroking.com/"
    "OpenAPI_File/files/OpenAPIScripMaster.json"
)

NIFTY_INDEX_TOKEN = "99926000"      # NIFTY 50 spot token (NSE indices)
NIFTY_INDEX_EXCH = "NSE"

SLEEP_BETWEEN_CALLS_SEC = 0.4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ORB")


# ---------- Filter config ---------------------------------------------------

@dataclass
class Filters:
    """All filter parameters in one place. None / empty = filter disabled."""
    min_bo_pct: float = 0.15          # min % above OR-high for entry candle close
    vol_mult: float = 1.5             # breakout vol >= vol_mult × avg OR vol
    skip_days: list[str] = field(default_factory=lambda: ["Friday"])
    use_nifty_filter: bool = True     # require Nifty also above its OR-high

    @classmethod
    def disabled(cls) -> "Filters":
        return cls(min_bo_pct=0.0, vol_mult=0.0, skip_days=[], use_nifty_filter=False)

    def describe(self) -> str:
        parts = []
        if self.min_bo_pct > 0:
            parts.append(f"min_bo>={self.min_bo_pct}%")
        if self.vol_mult > 0:
            parts.append(f"vol>={self.vol_mult}x")
        if self.skip_days:
            parts.append(f"skip={','.join(self.skip_days)}")
        if self.use_nifty_filter:
            parts.append("nifty_aligned")
        return " | ".join(parts) if parts else "NONE"


# ---------- Auth ------------------------------------------------------------

def login() -> SmartConnect:
    api_key = os.environ["ANGEL_API_KEY"]
    client_id = os.environ["ANGEL_CLIENT_ID"]
    pin = os.environ["ANGEL_PIN"]
    totp_secret = os.environ["ANGEL_TOTP_SECRET"]

    smart = SmartConnect(api_key=api_key)
    totp = pyotp.TOTP(totp_secret).now()
    resp = smart.generateSession(client_id, pin, totp)
    if not resp.get("status"):
        raise RuntimeError(f"SmartAPI login failed: {resp}")
    log.info("Logged in to SmartAPI as %s", client_id)
    return smart


# ---------- Symbol-token resolution ----------------------------------------

_scrip_master_cache: Optional[pd.DataFrame] = None


def get_symbol_token(trading_symbol: str, exchange: str = "NSE") -> str:
    global _scrip_master_cache
    if _scrip_master_cache is None:
        log.info("Downloading Angel One scrip master ...")
        df = pd.DataFrame(requests.get(SCRIP_MASTER_URL, timeout=30).json())
        _scrip_master_cache = df

    df = _scrip_master_cache
    sym = trading_symbol.upper()
    if exchange == "NSE" and not sym.endswith("-EQ"):
        sym = f"{sym}-EQ"
    row = df[(df["symbol"] == sym) & (df["exch_seg"] == exchange)]
    if row.empty:
        raise ValueError(f"Symbol {sym} not found on {exchange}")
    return str(row.iloc[0]["token"])


# ---------- Historical data — per-day fetch --------------------------------

def fetch_candles_one_day(
    smart: SmartConnect,
    symbol_token: str,
    exchange: str,
    interval: str,
    day: date,
) -> pd.DataFrame:
    params = {
        "exchange": exchange,
        "symboltoken": symbol_token,
        "interval": interval,
        "fromdate": f"{day.isoformat()} 09:15",
        "todate": f"{day.isoformat()} 15:30",
    }
    try:
        resp = smart.getCandleData(params)
    except Exception as e:
        log.warning("Candle fetch error %s: %s", day, e)
        return pd.DataFrame()

    if not resp.get("status") or not resp.get("data"):
        return pd.DataFrame()

    df = pd.DataFrame(
        resp["data"],
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(IST)
    df["date"] = df["timestamp"].dt.date
    df["time"] = df["timestamp"].dt.time
    return df


def fetch_candles_per_day(
    smart: SmartConnect,
    symbol_token: str,
    exchange: str,
    interval: str,
    start: date,
    end: date,
    label: str = "stock",
) -> pd.DataFrame:
    out = []
    cur = start
    days_processed = 0
    while cur <= end:
        if cur.weekday() < 5:  # weekdays only
            df = fetch_candles_one_day(smart, symbol_token, exchange, interval, cur)
            if not df.empty:
                out.append(df)
                days_processed += 1
                if days_processed % 10 == 0:
                    log.info("[%s] fetched %d trading days (last: %s)",
                             label, days_processed, cur)
            time.sleep(SLEEP_BETWEEN_CALLS_SEC)
        cur += timedelta(days=1)

    if not out:
        return pd.DataFrame()
    return (
        pd.concat(out, ignore_index=True)
          .drop_duplicates(subset=["timestamp"])
          .sort_values("timestamp")
          .reset_index(drop=True)
    )


# ---------- Trade record ----------------------------------------------------

@dataclass
class Trade:
    date: str
    entry_time: str
    entry_price: float
    exit_time: str
    exit_price: float
    qty: int
    pnl: float
    return_pct: float
    exit_reason: str
    or_high: float
    or_low: float
    r_multiple: float
    breakout_pct: float        # how far above OR-high the entry-trigger candle closed
    vol_ratio: float           # breakout candle vol / avg OR vol
    skipped_reason: str = ""   # populated when no trade taken


# ---------- Per-day simulation ---------------------------------------------

def simulate_day(
    day_df: pd.DataFrame,
    nifty_day: Optional[pd.DataFrame],
    capital: float,
    risk_pct: float,
    filters: Filters,
) -> Optional[Trade]:
    if day_df.empty:
        return None

    # Day-of-week filter ----------------------------------------------------
    day_obj = day_df["timestamp"].iloc[0]
    dow = day_obj.day_name()
    if dow in filters.skip_days:
        return None  # filter B fired silently

    # Build opening range ---------------------------------------------------
    or_candles = day_df[day_df["time"] < OR_END]
    if or_candles.empty:
        return None
    or_high = float(or_candles["high"].max())
    or_low = float(or_candles["low"].min())
    open_px = float(or_candles["open"].iloc[0])
    width_pct = (or_high - or_low) / open_px * 100
    if width_pct < MIN_OR_WIDTH_PCT:
        return None
    avg_or_vol = float(or_candles["volume"].mean()) or 1.0

    # Nifty alignment - precompute Nifty's OR-high ---------------------------
    nifty_or_high = None
    if filters.use_nifty_filter:
        if nifty_day is None or nifty_day.empty:
            return None  # no Nifty data → don't trade
        nifty_or = nifty_day[nifty_day["time"] < OR_END]
        if nifty_or.empty:
            return None
        nifty_or_high = float(nifty_or["high"].max())

    # Watch for breakout ----------------------------------------------------
    post = day_df[
        (day_df["time"] >= OR_END) & (day_df["time"] < NO_NEW_ENTRY_AFTER)
    ].reset_index(drop=True)

    breakout_idx = None
    breakout_candle = None
    for i, row in post.iterrows():
        if row["close"] <= or_high:
            continue

        # Filter A: breakout strength
        bo_pct = (row["close"] - or_high) / or_high * 100
        if bo_pct < filters.min_bo_pct:
            continue

        # Filter C: volume confirmation
        vol_ratio = row["volume"] / avg_or_vol
        if filters.vol_mult > 0 and vol_ratio < filters.vol_mult:
            continue

        # Filter D: Nifty must also be above its OR-high at this same bar
        if filters.use_nifty_filter and nifty_day is not None:
            nifty_now = nifty_day[nifty_day["timestamp"] <= row["timestamp"]]
            if nifty_now.empty:
                continue
            nifty_close_now = float(nifty_now.iloc[-1]["close"])
            if nifty_close_now <= nifty_or_high:
                continue

        breakout_idx = i
        breakout_candle = row
        break

    if breakout_idx is None:
        return None

    # Entry at NEXT bar's open (no look-ahead) -----------------------------
    if breakout_idx + 1 >= len(post):
        return None
    entry_row = post.iloc[breakout_idx + 1]
    entry_price = float(entry_row["open"])

    sl_distance = entry_price - or_low
    if sl_distance <= 0:
        return None
    target = entry_price + TARGET_R_MULTIPLE * sl_distance
    risk_amount = capital * (risk_pct / 100.0)
    qty = max(1, int(risk_amount // sl_distance))

    bo_pct = (breakout_candle["close"] - or_high) / or_high * 100
    vol_ratio = breakout_candle["volume"] / avg_or_vol

    # Walk forward through full session ------------------------------------
    trail = day_df[day_df["timestamp"] > entry_row["timestamp"]].reset_index(drop=True)
    for _, row in trail.iterrows():
        # Conservative: stop-first if both touched in same bar
        if row["low"] <= or_low:
            return _trade(entry_row, entry_price, row, or_low, qty,
                          "stop_hit", or_high, or_low, -1.0, bo_pct, vol_ratio)
        if row["high"] >= target:
            return _trade(entry_row, entry_price, row, target, qty,
                          "target_hit", or_high, or_low, TARGET_R_MULTIPLE,
                          bo_pct, vol_ratio)
        if row["time"] >= SQUARE_OFF:
            close_px = float(row["close"])
            return _trade(entry_row, entry_price, row, close_px, qty,
                          "end_of_day", or_high, or_low,
                          (close_px - entry_price) / sl_distance,
                          bo_pct, vol_ratio)

    last = trail.iloc[-1] if not trail.empty else entry_row
    close_px = float(last["close"])
    return _trade(entry_row, entry_price, last, close_px, qty,
                  "data_end", or_high, or_low,
                  (close_px - entry_price) / sl_distance,
                  bo_pct, vol_ratio)


def _trade(entry_row, entry_price, exit_row, exit_price, qty, reason,
           or_h, or_l, r_mult, bo_pct, vol_ratio) -> Trade:
    return Trade(
        date=str(exit_row["date"]),
        entry_time=str(entry_row["timestamp"]),
        entry_price=entry_price,
        exit_time=str(exit_row["timestamp"]),
        exit_price=exit_price,
        qty=qty,
        pnl=(exit_price - entry_price) * qty,
        return_pct=(exit_price - entry_price) / entry_price * 100,
        exit_reason=reason,
        or_high=or_h, or_low=or_l,
        r_multiple=r_mult,
        breakout_pct=bo_pct,
        vol_ratio=vol_ratio,
    )


# ---------- Reporting -------------------------------------------------------

def report(trades: list[Trade], symbol: str, capital: float,
           filters: Filters) -> pd.DataFrame:
    if not trades:
        log.info("No trades generated under these filters.")
        return pd.DataFrame()

    df = pd.DataFrame([t.__dict__ for t in trades]).sort_values("date").reset_index(drop=True)
    wins = df[df["pnl"] > 0]
    losses = df[df["pnl"] <= 0]
    total = df["pnl"].sum()
    win_rate = len(wins) / len(df) * 100
    avg_win = wins["pnl"].mean() if len(wins) else 0
    avg_loss = losses["pnl"].mean() if len(losses) else 0
    expectancy = df["pnl"].mean()
    avg_r = df["r_multiple"].mean()

    df["cum_pnl"] = df["pnl"].cumsum()
    df["peak"] = df["cum_pnl"].cummax()
    df["dd"] = df["cum_pnl"] - df["peak"]
    max_dd = df["dd"].min()

    profit_factor = (
        wins["pnl"].sum() / abs(losses["pnl"].sum())
        if len(losses) and losses["pnl"].sum() != 0 else float("inf")
    )

    print("\n" + "=" * 70)
    print(f"ORB Backtest Results — {symbol}")
    print(f"Filters applied: {filters.describe()}")
    print("=" * 70)
    print(f"Capital              : ₹{capital:,.0f}")
    print(f"Trades               : {len(df)}")
    print(f"Win rate             : {win_rate:.1f}% ({len(wins)}W / {len(losses)}L)")
    print(f"Avg win              : ₹{avg_win:,.2f}")
    print(f"Avg loss             : ₹{avg_loss:,.2f}")
    print(f"Avg R per trade      : {avg_r:.3f}")
    print(f"Expectancy           : ₹{expectancy:,.2f} per trade")
    print(f"Profit factor        : {profit_factor:.2f}")
    print(f"Total P&L            : ₹{total:,.2f} ({total/capital*100:.2f}% on capital)")
    print(f"Max drawdown         : ₹{max_dd:,.2f} ({max_dd/capital*100:.2f}%)")
    print(f"\nExit reason breakdown:")
    print(df["exit_reason"].value_counts().to_string())
    print("=" * 70)
    return df


# ---------- Main ------------------------------------------------------------

def run_backtest(
    symbol: str,
    exchange: str,
    days_back: int,
    capital: float,
    risk_pct: float,
    filters: Filters,
    output_csv: Optional[str] = None,
) -> pd.DataFrame:

    smart = login()

    # Stock candles
    token = get_symbol_token(symbol, exchange)
    log.info("Symbol %s on %s → token %s", symbol, exchange, token)

    end = datetime.now(IST).date()
    start = end - timedelta(days=days_back)

    log.info("Fetching %s candles from %s to %s ...", symbol, start, end)
    stock_candles = fetch_candles_per_day(
        smart, token, exchange, "FIVE_MINUTE", start, end, label=symbol,
    )
    if stock_candles.empty:
        log.error("No candle data fetched — aborting.")
        return pd.DataFrame()
    log.info("Got %d candles for %s across %d days",
             len(stock_candles), symbol, stock_candles["date"].nunique())

    # Nifty candles (only if filter enabled)
    nifty_by_day: dict[date, pd.DataFrame] = {}
    if filters.use_nifty_filter:
        log.info("Fetching NIFTY 50 index candles for alignment filter ...")
        nifty_candles = fetch_candles_per_day(
            smart, NIFTY_INDEX_TOKEN, NIFTY_INDEX_EXCH, "FIVE_MINUTE",
            start, end, label="NIFTY",
        )
        if nifty_candles.empty:
            log.warning("Nifty fetch empty — disabling alignment filter.")
            filters.use_nifty_filter = False
        else:
            for d, df in nifty_candles.groupby("date"):
                nifty_by_day[d] = df.reset_index(drop=True)
            log.info("Got Nifty data for %d days", len(nifty_by_day))

    # Simulate day-by-day
    trades = []
    for d, day_df in stock_candles.groupby("date"):
        nifty_day = nifty_by_day.get(d) if filters.use_nifty_filter else None
        t = simulate_day(
            day_df.reset_index(drop=True),
            nifty_day, capital, risk_pct, filters,
        )
        if t:
            trades.append(t)

    df = report(trades, symbol, capital, filters)

    if not df.empty:
        path = output_csv or f"backtest_{symbol}.csv"
        df.to_csv(path, index=False)
        log.info("Trade log saved → %s", path)

    return df


# ---------- CLI -------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ORB intraday strategy backtest using Angel One SmartAPI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("-s", "--symbol", required=True,
                   help="Trading symbol, e.g. RELIANCE, HDFCBANK, INFY")
    p.add_argument("-e", "--exchange", default="NSE",
                   help="Exchange (NSE/BSE)")
    p.add_argument("-d", "--days", type=int, default=120,
                   help="How many calendar days of history to backtest")
    p.add_argument("-c", "--capital", type=float, default=100_000,
                   help="Capital in INR for sizing simulation")
    p.add_argument("-r", "--risk-pct", type=float, default=1.0,
                   help="Risk %% of capital per trade (= SL distance × qty)")

    # Filter controls
    p.add_argument("--min-bo-pct", type=float, default=0.15,
                   help="Min %% breakout candle close must be above OR-high "
                        "(0 = disabled). Filters out fakeouts.")
    p.add_argument("--vol-mult", type=float, default=1.5,
                   help="Min breakout-candle volume ratio vs avg OR-candle "
                        "volume (0 = disabled).")
    p.add_argument("--skip-days", nargs="*", default=["Friday"],
                   choices=["Monday", "Tuesday", "Wednesday", "Thursday",
                            "Friday"],
                   help="Days of week to skip entirely.")
    p.add_argument("--no-nifty-filter", action="store_true",
                   help="Disable Nifty alignment filter (saves API calls).")
    p.add_argument("--no-filters", action="store_true",
                   help="Disable ALL filters → baseline behaviour.")

    p.add_argument("-o", "--output",
                   help="Output CSV path (default: backtest_<SYMBOL>.csv)")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.no_filters:
        filters = Filters.disabled()
    else:
        filters = Filters(
            min_bo_pct=args.min_bo_pct,
            vol_mult=args.vol_mult,
            skip_days=list(args.skip_days),
            use_nifty_filter=not args.no_nifty_filter,
        )

    log.info("Filters: %s", filters.describe())

    run_backtest(
        symbol=args.symbol,
        exchange=args.exchange,
        days_back=args.days,
        capital=args.capital,
        risk_pct=args.risk_pct,
        filters=filters,
        output_csv=args.output,
    )


if __name__ == "__main__":
    main()