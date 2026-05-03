"""
Bulk ORB Backtest — runs the strategy on 15 stocks, generates a single
comprehensive report.

Built to respect Angel SmartAPI rate limits:
    - Max 3 requests/second on getCandleData (we cap at 2.5)
    - Max 180 requests/minute (we cap at 170)
    - Auto-retry with exponential backoff on 403/rate-limit
    - Cached Nifty fetch (downloaded ONCE, reused across all 15 stocks)
    - Cached candle data per stock (resume if interrupted)
    - Single login session for the entire bulk run

CLI:
    python bulk_backtest.py                          # all 15 stocks, filtered
    python bulk_backtest.py --no-filters             # baseline
    python bulk_backtest.py --days 180               # more history
    python bulk_backtest.py --symbols RELIANCE INFY  # subset
    python bulk_backtest.py --report-only            # regenerate report from cache

Output:
    cache/<SYMBOL>_5min_<start>_<end>.parquet   - cached candle data
    cache/NIFTY_5min_<start>_<end>.parquet      - cached Nifty data
    trades_<SYMBOL>.csv                         - per-stock trade log
    bulk_report.md                              - SINGLE comprehensive report
    bulk_report.csv                             - leaderboard CSV
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, time as dtime, timedelta, date
from pathlib import Path
from typing import Optional

import pandas as pd
import pyotp
import requests
from dotenv import load_dotenv
from SmartApi import SmartConnect

load_dotenv()


# ---------- Constants -------------------------------------------------------

IST_OFFSET_HOURS = 5.5
OR_END = dtime(9, 45)
NO_NEW_ENTRY_AFTER = dtime(14, 30)
SQUARE_OFF = dtime(15, 15)
TARGET_R_MULTIPLE = 1.5
MIN_OR_WIDTH_PCT = 0.20

SCRIP_MASTER_URL = (
    "https://margincalculator.angelbroking.com/"
    "OpenAPI_File/files/OpenAPIScripMaster.json"
)
NIFTY_INDEX_TOKEN = "99926000"
NIFTY_INDEX_EXCH = "NSE"

# Rate limit settings (well under SmartAPI's 3/sec, 180/min for getCandleData)
MAX_REQ_PER_SEC = 2.5
MAX_REQ_PER_MIN = 170
RETRY_BASE_DELAY = 2.0
MAX_RETRIES = 5

CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)


# Curated 15 large-cap stocks across sectors with high intraday liquidity
DEFAULT_STOCKS = [
    # Banking & financials (5)
    "HDFCBANK", "ICICIBANK", "SBIN", "AXISBANK", "BAJFINANCE",
    # IT (2)
    "INFY", "TCS",
    # Energy / conglomerate (1)
    "RELIANCE",
    # Capex / infra (2)
    "LT", "ADANIPORTS",
    # Auto (2)
    "MARUTI", "TATAMOTORS",
    # Metals (1)
    "HINDALCO",
    # Consumer (1)
    "TITAN",
    # Pharma (1)
    "DRREDDY",
]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("BULK")


# ---------- Filter config ---------------------------------------------------

@dataclass
class Filters:
    min_bo_pct: float = 0.15
    vol_mult: float = 1.5
    skip_days: list[str] = field(default_factory=lambda: ["Friday"])
    use_nifty_filter: bool = True

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


# ---------- Rate limiter ----------------------------------------------------

class RateLimiter:
    """Enforces both per-second and per-minute API caps."""

    def __init__(self, per_sec: float, per_min: int):
        self.per_sec = per_sec
        self.per_min = per_min
        self.recent: deque[float] = deque()

    def wait(self) -> None:
        now = time.time()
        while self.recent and now - self.recent[0] > 60:
            self.recent.popleft()

        if len(self.recent) >= self.per_min:
            sleep_for = 60 - (now - self.recent[0]) + 0.1
            log.info("Per-min cap reached, sleeping %.1fs", sleep_for)
            time.sleep(max(0.1, sleep_for))
            return self.wait()

        if self.recent:
            elapsed = now - self.recent[-1]
            min_gap = 1.0 / self.per_sec
            if elapsed < min_gap:
                time.sleep(min_gap - elapsed)

        self.recent.append(time.time())


limiter = RateLimiter(per_sec=MAX_REQ_PER_SEC, per_min=MAX_REQ_PER_MIN)


# ---------- Auth + scrip master --------------------------------------------

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
    log.info("Logged in as %s", client_id)
    return smart


_scrip_cache: Optional[pd.DataFrame] = None


def get_symbol_token(symbol: str, exchange: str = "NSE") -> str:
    global _scrip_cache
    if _scrip_cache is None:
        log.info("Downloading Angel scrip master ...")
        _scrip_cache = pd.DataFrame(requests.get(SCRIP_MASTER_URL, timeout=30).json())

    sym = symbol.upper()
    if exchange == "NSE" and not sym.endswith("-EQ"):
        sym = f"{sym}-EQ"
    row = _scrip_cache[(_scrip_cache["symbol"] == sym) & (_scrip_cache["exch_seg"] == exchange)]
    if row.empty:
        raise ValueError(f"Symbol {sym} not found on {exchange}")
    return str(row.iloc[0]["token"])


# ---------- Candle fetch with retry & rate limiting ------------------------

def fetch_one_day(
    smart: SmartConnect, symbol_token: str, exchange: str, day: date,
) -> pd.DataFrame:
    params = {
        "exchange": exchange,
        "symboltoken": symbol_token,
        "interval": "FIVE_MINUTE",
        "fromdate": f"{day.isoformat()} 09:15",
        "todate": f"{day.isoformat()} 15:30",
    }

    for attempt in range(MAX_RETRIES):
        limiter.wait()
        try:
            resp = smart.getCandleData(params)
        except Exception as e:
            err = str(e).lower()
            if "rate" in err or "exceed" in err or "403" in err:
                wait = RETRY_BASE_DELAY * (2 ** attempt)
                log.warning("Rate-limit on %s, retry %d/%d in %.1fs",
                            day, attempt + 1, MAX_RETRIES, wait)
                time.sleep(wait)
                continue
            log.warning("Fetch error %s: %s", day, e)
            return pd.DataFrame()

        if not resp.get("status"):
            err_msg = (resp.get("message") or "").lower()
            if "exceed" in err_msg or "rate" in err_msg:
                wait = RETRY_BASE_DELAY * (2 ** attempt)
                log.warning("Rate-limit response on %s, retry in %.1fs", day, wait)
                time.sleep(wait)
                continue
            return pd.DataFrame()

        if not resp.get("data"):
            return pd.DataFrame()

        df = pd.DataFrame(resp["data"], columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["timestamp"] = df["timestamp"] + pd.Timedelta(hours=IST_OFFSET_HOURS)
        df["timestamp"] = df["timestamp"].dt.tz_localize(None)
        df["date"] = df["timestamp"].dt.date
        df["time"] = df["timestamp"].dt.time
        return df

    log.error("Gave up on %s after %d retries", day, MAX_RETRIES)
    return pd.DataFrame()


def fetch_or_load_cached(
    smart: SmartConnect, symbol: str, symbol_token: str, exchange: str,
    start: date, end: date, label: str,
) -> pd.DataFrame:
    """Cached fetch — saves to parquet, loads if file exists."""
    cache_path = CACHE_DIR / f"{symbol}_5min_{start}_{end}.parquet"
    if cache_path.exists():
        try:
            df = pd.read_parquet(cache_path)
            df["date"] = df["timestamp"].dt.date
            df["time"] = df["timestamp"].dt.time
            log.info("[%s] cache hit → %d candles", label, len(df))
            return df
        except Exception as e:
            log.warning("Cache read failed for %s: %s — refetching", symbol, e)

    log.info("[%s] fetching %s → %s ...", label, start, end)
    out = []
    cur = start
    days_done = 0
    while cur <= end:
        if cur.weekday() < 5:
            df = fetch_one_day(smart, symbol_token, exchange, cur)
            if not df.empty:
                out.append(df)
                days_done += 1
                if days_done % 10 == 0:
                    log.info("  [%s] fetched %d days (last: %s)", label, days_done, cur)
        cur += timedelta(days=1)

    if not out:
        log.error("[%s] empty fetch", label)
        return pd.DataFrame()

    full = (pd.concat(out, ignore_index=True)
              .drop_duplicates(subset=["timestamp"])
              .sort_values("timestamp")
              .reset_index(drop=True))

    cache_df = full[["timestamp", "open", "high", "low", "close", "volume"]].copy()
    cache_df.to_parquet(cache_path, index=False)
    log.info("[%s] cached → %s (%d candles)", label, cache_path.name, len(full))
    return full


# ---------- Trade simulation ------------------------------------------------

@dataclass
class Trade:
    symbol: str
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
    breakout_pct: float
    vol_ratio: float


def simulate_day(
    symbol: str, day_df: pd.DataFrame, nifty_day: Optional[pd.DataFrame],
    capital: float, risk_pct: float, filters: Filters,
) -> Optional[Trade]:
    if day_df.empty:
        return None

    dow = day_df["timestamp"].iloc[0].day_name()
    if dow in filters.skip_days:
        return None

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

    nifty_or_high = None
    if filters.use_nifty_filter:
        if nifty_day is None or nifty_day.empty:
            return None
        nifty_or = nifty_day[nifty_day["time"] < OR_END]
        if nifty_or.empty:
            return None
        nifty_or_high = float(nifty_or["high"].max())

    post = day_df[
        (day_df["time"] >= OR_END) & (day_df["time"] < NO_NEW_ENTRY_AFTER)
    ].reset_index(drop=True)

    breakout_idx = None
    breakout_candle = None
    for i, row in post.iterrows():
        if row["close"] <= or_high:
            continue
        bo_pct = (row["close"] - or_high) / or_high * 100
        if bo_pct < filters.min_bo_pct:
            continue
        vol_ratio = row["volume"] / avg_or_vol
        if filters.vol_mult > 0 and vol_ratio < filters.vol_mult:
            continue
        if filters.use_nifty_filter and nifty_day is not None:
            nifty_now = nifty_day[nifty_day["timestamp"] <= row["timestamp"]]
            if nifty_now.empty:
                continue
            if float(nifty_now.iloc[-1]["close"]) <= nifty_or_high:
                continue
        breakout_idx = i
        breakout_candle = row
        break

    if breakout_idx is None or breakout_idx + 1 >= len(post):
        return None

    entry_row = post.iloc[breakout_idx + 1]
    entry_price = float(entry_row["open"])
    sl_distance = entry_price - or_low
    if sl_distance <= 0:
        return None
    target = entry_price + TARGET_R_MULTIPLE * sl_distance
    qty = max(1, int((capital * (risk_pct / 100.0)) // sl_distance))

    bo_pct = (breakout_candle["close"] - or_high) / or_high * 100
    vol_ratio = breakout_candle["volume"] / avg_or_vol

    trail = day_df[day_df["timestamp"] > entry_row["timestamp"]].reset_index(drop=True)
    for _, row in trail.iterrows():
        if row["low"] <= or_low:
            return _mk_trade(symbol, entry_row, entry_price, row, or_low, qty,
                             "stop_hit", or_high, or_low, -1.0, bo_pct, vol_ratio)
        if row["high"] >= target:
            return _mk_trade(symbol, entry_row, entry_price, row, target, qty,
                             "target_hit", or_high, or_low, TARGET_R_MULTIPLE,
                             bo_pct, vol_ratio)
        if row["time"] >= SQUARE_OFF:
            close_px = float(row["close"])
            return _mk_trade(symbol, entry_row, entry_price, row, close_px, qty,
                             "end_of_day", or_high, or_low,
                             (close_px - entry_price) / sl_distance,
                             bo_pct, vol_ratio)

    if not trail.empty:
        last = trail.iloc[-1]
        close_px = float(last["close"])
        return _mk_trade(symbol, entry_row, entry_price, last, close_px, qty,
                         "data_end", or_high, or_low,
                         (close_px - entry_price) / sl_distance,
                         bo_pct, vol_ratio)
    return None


def _mk_trade(symbol, entry_row, entry_price, exit_row, exit_price, qty, reason,
              or_h, or_l, r_mult, bo_pct, vol_ratio) -> Trade:
    return Trade(
        symbol=symbol,
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


# ---------- Per-stock backtest ---------------------------------------------

def backtest_symbol(
    smart: SmartConnect, symbol: str, exchange: str,
    start: date, end: date, capital: float, risk_pct: float,
    filters: Filters, nifty_by_day: dict,
) -> pd.DataFrame:

    try:
        token = get_symbol_token(symbol, exchange)
    except Exception as e:
        log.error("[%s] token lookup failed: %s", symbol, e)
        return pd.DataFrame()

    candles = fetch_or_load_cached(
        smart, symbol, token, exchange, start, end, label=symbol,
    )
    if candles.empty:
        return pd.DataFrame()

    trades = []
    for d, day_df in candles.groupby("date"):
        nifty_day = nifty_by_day.get(d) if filters.use_nifty_filter else None
        t = simulate_day(symbol, day_df.reset_index(drop=True), nifty_day,
                         capital, risk_pct, filters)
        if t:
            trades.append(t)

    if not trades:
        return pd.DataFrame()

    df = pd.DataFrame([t.__dict__ for t in trades]).sort_values("date").reset_index(drop=True)
    df.to_csv(f"trades_{symbol}.csv", index=False)
    return df


# ---------- Reporting -------------------------------------------------------

def compute_stats(df: pd.DataFrame, capital: float) -> dict:
    if df.empty:
        return {"trades": 0}

    wins = df[df["pnl"] > 0]
    losses = df[df["pnl"] <= 0]
    total = df["pnl"].sum()
    win_rate = len(wins) / len(df) * 100
    pf = (
        wins["pnl"].sum() / abs(losses["pnl"].sum())
        if len(losses) and losses["pnl"].sum() != 0
        else float("inf")
    )

    df = df.sort_values("date").reset_index(drop=True)
    df["cum"] = df["pnl"].cumsum()
    df["dd"] = df["cum"] - df["cum"].cummax()
    max_dd = df["dd"].min()
    target_pct = (df["exit_reason"] == "target_hit").mean() * 100

    return {
        "trades": len(df),
        "wins": len(wins),
        "losses": len(losses),
        "win_pct": round(win_rate, 1),
        "avg_win": round(wins["pnl"].mean() if len(wins) else 0, 0),
        "avg_loss": round(losses["pnl"].mean() if len(losses) else 0, 0),
        "expectancy": round(df["pnl"].mean(), 0),
        "profit_factor": round(pf, 2) if pf != float("inf") else 999.0,
        "total_pnl": round(total, 0),
        "total_return_pct": round(total / capital * 100, 2),
        "max_dd": round(max_dd, 0),
        "max_dd_pct": round(max_dd / capital * 100, 2),
        "target_hit_pct": round(target_pct, 1),
        "avg_r": round(df["r_multiple"].mean(), 3),
    }


def grade(stats: dict) -> str:
    if stats["trades"] < 15:
        return "❌ TOO FEW TRADES"
    if stats["profit_factor"] < 1.0:
        return "❌ UNPROFITABLE"
    if stats["max_dd_pct"] < -5.0:
        return "⚠ HIGH DRAWDOWN"
    if stats["profit_factor"] < 1.3:
        return "⚠ MARGINAL EDGE"
    if stats["target_hit_pct"] < 5.0:
        return "⚠ NOT REACHING TARGET"
    return "✓ CANDIDATE"


def write_report(
    results: dict[str, pd.DataFrame], capital: float, filters: Filters,
    days_back: int, period_start: date, period_end: date,
) -> None:
    rows = []
    for sym, df in results.items():
        s = compute_stats(df, capital)
        s["symbol"] = sym
        s["verdict"] = grade(s) if s["trades"] > 0 else "❌ NO TRADES"
        rows.append(s)

    summary = pd.DataFrame(rows)
    if summary.empty:
        log.error("No results to report.")
        return

    cols = ["symbol", "trades", "win_pct", "profit_factor", "expectancy",
            "total_pnl", "total_return_pct", "max_dd_pct", "target_hit_pct",
            "avg_r", "verdict"]
    summary = summary.reindex(columns=cols).sort_values(
        "profit_factor", ascending=False,
    ).reset_index(drop=True)

    summary.to_csv("bulk_report.csv", index=False)
    log.info("Leaderboard saved → bulk_report.csv")

    # Markdown report
    lines = []
    lines.append(f"# ORB Bulk Backtest Report")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now():%Y-%m-%d %H:%M IST}  ")
    lines.append(f"**Period:** {period_start} → {period_end} ({days_back} days)  ")
    lines.append(f"**Capital per stock:** ₹{capital:,.0f}  ")
    lines.append(f"**Filters:** `{filters.describe()}`  ")
    lines.append(f"**Stocks tested:** {len(results)}")
    lines.append("")
    lines.append("## Strategy")
    lines.append("")
    lines.append("30-min Opening Range Breakout (long-only):")
    lines.append("- Build OR from 9:15-9:45 IST (5-min candles)")
    lines.append("- Entry: long at next bar's open after 5-min close above OR-high")
    lines.append("- Stop: OR-low; Target: 1.5R fixed; Square-off: 15:15 IST")
    lines.append("- One trade per symbol per day")
    lines.append("")
    lines.append("## Leaderboard (sorted by Profit Factor)")
    lines.append("")
    lines.append("| Symbol | Trades | Win% | PF | Expectancy ₹ | Total ₹ | Total % | MaxDD % | Target% | Avg R | Verdict |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
    for _, r in summary.iterrows():
        lines.append(
            f"| {r['symbol']} | {r['trades']} | {r['win_pct']}% | "
            f"{r['profit_factor']} | {r['expectancy']:,.0f} | "
            f"{r['total_pnl']:,.0f} | {r['total_return_pct']}% | "
            f"{r['max_dd_pct']}% | {r['target_hit_pct']}% | "
            f"{r['avg_r']} | {r['verdict']} |"
        )
    lines.append("")

    candidates = summary[summary["verdict"] == "✓ CANDIDATE"]
    lines.append("## Verdict Summary")
    lines.append("")
    if len(candidates) >= 3:
        lines.append(f"✓ **{len(candidates)} candidate stock(s) found.** Suggested live universe:")
        for _, r in candidates.head(5).iterrows():
            lines.append(f"- **{r['symbol']}** — PF {r['profit_factor']}, "
                        f"{r['win_pct']}% win, {r['trades']} trades, "
                        f"{r['total_return_pct']}% total return")
        lines.append("")
        lines.append("Consider running **out-of-sample validation** (split data 60/40) "
                    "before going live with these.")
    elif len(candidates) > 0:
        lines.append(f"⚠ Only {len(candidates)} candidate(s) passed all bars. Sample is thin.")
        lines.append("Recommend extending the backtest period (--days 240) before deciding.")
    else:
        lines.append("❌ **No stocks passed all four bars.**")
        lines.append("")
        lines.append("Possible interpretations:")
        lines.append("1. **Market regime is wrong for ORB** — Indian large caps may be range-bound")
        lines.append("   in this period rather than trending. ORB needs trends.")
        lines.append("2. **Filters too strict** — try `--no-filters` to see baseline.")
        lines.append("3. **Period too short** — extend to `--days 240` for more sample.")
        lines.append("")
        lines.append("Alternative strategies worth testing in a non-trending regime:")
        lines.append("- **VWAP mean-reversion** (fade extremes back to VWAP)")
        lines.append("- **Range breakout with shorter target** (1R instead of 1.5R)")
        lines.append("- **Gap-and-go** (only trade days with 0.5%+ gap up/down)")
    lines.append("")

    # Per-stock details
    lines.append("## Per-Stock Details")
    lines.append("")
    for _, r in summary.iterrows():
        lines.append(f"### {r['symbol']} — {r['verdict']}")
        lines.append("")
        lines.append(f"- Trades: {r['trades']} ({r['win_pct']}% win rate)")
        lines.append(f"- Profit factor: {r['profit_factor']}")
        lines.append(f"- Expectancy: ₹{r['expectancy']:,.0f} per trade")
        lines.append(f"- Total P&L: ₹{r['total_pnl']:,.0f} ({r['total_return_pct']}% on capital)")
        lines.append(f"- Max drawdown: {r['max_dd_pct']}% of capital")
        lines.append(f"- Target hit rate: {r['target_hit_pct']}%")
        lines.append(f"- Avg R per trade: {r['avg_r']}")
        lines.append("")

    lines.append("## Reading the Report")
    lines.append("")
    lines.append("**Profit Factor (PF)** = sum of wins / sum of losses. PF > 1 means profitable. ")
    lines.append("PF > 1.5 is a real edge. PF > 2 is excellent.  ")
    lines.append("**Expectancy** = average ₹ per trade. The single most important metric.  ")
    lines.append("**Max DD** = peak-to-trough loss. Survival metric. < 5% of capital = manageable.  ")
    lines.append("**Target%** = % of trades hitting 1.5R target. < 5% means the strategy isn't working as designed.  ")
    lines.append("**Avg R** = average trade outcome in R units. > 0.2 is solid.")
    lines.append("")
    lines.append("**Verdict bars (all must pass for ✓ CANDIDATE):**")
    lines.append("- Trades ≥ 15 (statistical credibility)")
    lines.append("- Profit factor ≥ 1.3 (real edge)")
    lines.append("- Max DD ≥ -5% of capital (survivable)")
    lines.append("- Target% ≥ 5% (strategy reaching design intent)")
    lines.append("")
    lines.append("---")
    lines.append("*Always validate out-of-sample before risking real capital.*")

    Path("bulk_report.md").write_text("\n".join(lines))
    log.info("Report saved → bulk_report.md")

    print("\n\n" + "=" * 80)
    print("BULK BACKTEST RESULTS")
    print("=" * 80)
    print(summary.to_string(index=False))
    print("\nFull report → bulk_report.md")
    print(f"Candidate stocks: {list(candidates['symbol'])}")


# ---------- Main ------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(
        description="Bulk ORB backtest on 15 stocks with rate-limit-safe Angel API calls",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--days", type=int, default=120,
                   help="Days of history per stock")
    p.add_argument("--capital", type=float, default=100_000)
    p.add_argument("--risk-pct", type=float, default=1.0)
    p.add_argument("--symbols", nargs="*", default=DEFAULT_STOCKS,
                   help="Override stock list")
    p.add_argument("--no-filters", action="store_true")
    p.add_argument("--min-bo-pct", type=float, default=0.15)
    p.add_argument("--vol-mult", type=float, default=1.5)
    p.add_argument("--skip-days", nargs="*", default=["Friday"])
    p.add_argument("--no-nifty-filter", action="store_true")
    p.add_argument("--report-only", action="store_true",
                   help="Skip fetching/simulation, regenerate report from existing trades_*.csv")
    args = p.parse_args()

    if args.no_filters:
        filters = Filters.disabled()
    else:
        filters = Filters(
            min_bo_pct=args.min_bo_pct,
            vol_mult=args.vol_mult,
            skip_days=list(args.skip_days),
            use_nifty_filter=not args.no_nifty_filter,
        )

    end = datetime.now().date()
    start = end - timedelta(days=args.days)

    log.info("Bulk backtest: %d stocks, %d days (%s → %s)",
             len(args.symbols), args.days, start, end)
    log.info("Filters: %s", filters.describe())

    if args.report_only:
        results = {}
        for sym in args.symbols:
            csv = Path(f"trades_{sym}.csv")
            if csv.exists():
                results[sym] = pd.read_csv(csv)
        write_report(results, args.capital, filters, args.days, start, end)
        return

    smart = login()

    # Fetch Nifty ONCE if filter enabled
    nifty_by_day: dict = {}
    if filters.use_nifty_filter:
        log.info("Pre-fetching Nifty (one-time, shared across all stocks)...")
        nifty_df = fetch_or_load_cached(
            smart, "NIFTY", NIFTY_INDEX_TOKEN, NIFTY_INDEX_EXCH,
            start, end, label="NIFTY",
        )
        if nifty_df.empty:
            log.warning("Nifty fetch failed — disabling alignment filter")
            filters.use_nifty_filter = False
        else:
            for d, df in nifty_df.groupby("date"):
                nifty_by_day[d] = df.reset_index(drop=True)
            log.info("Nifty cached for %d days", len(nifty_by_day))

    results = {}
    for i, sym in enumerate(args.symbols, 1):
        log.info("[%d/%d] Processing %s ...", i, len(args.symbols), sym)
        try:
            df = backtest_symbol(
                smart, sym, "NSE", start, end,
                args.capital, args.risk_pct, filters, nifty_by_day,
            )
            results[sym] = df
            stats = compute_stats(df, args.capital)
            log.info("  → %d trades, PF %s, P&L ₹%s",
                     stats.get("trades", 0),
                     stats.get("profit_factor", "n/a"),
                     stats.get("total_pnl", "n/a"))
        except Exception as e:
            log.exception("  Failed on %s: %s", sym, e)
            results[sym] = pd.DataFrame()

    write_report(results, args.capital, filters, args.days, start, end)


if __name__ == "__main__":
    main()