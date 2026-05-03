# Angel ORB Backtest

30-minute Opening Range Breakout (ORB) backtesting system for Indian stocks using the Angel One SmartAPI.

## What it does

Simulates a long-only ORB strategy on NSE stocks: buys when price breaks out of the first 30 minutes of the trading day (9:15–9:45 IST), with a fixed 1.5R target and OR-low stop loss. Strictly intraday, one trade per stock per day, square-off at 15:15 IST.

Strategy is based on a published Nifty backtest (Jul–Oct 2025) showing 57% win rate with 1.5R fixed target — the simplest of four ORB variants tested, with the best expectancy.

## Project structure

```
.
├── angel_orb_backtest.py    # Single-stock backtest
├── bulk_backtest.py         # 15-stock bulk backtest with rate limiting
├── requirements.txt
├── env.example
└── cache/                   # cached candle data (gitignored)
```

## Setup

```bash
git clone <repo-url>
cd <repo-dir>
pip install -r requirements.txt
cp env.example .env
# edit .env with your Angel One credentials
```

### Required credentials (Angel One SmartAPI)

- Open a free Angel One demat account
- Enable TOTP at https://smartapi.angelone.in/enable-totp (save the QR seed string)
- Create a Market Feeds API at https://smartapi.angelone.in/
- Add to `.env`:
  ```
  ANGEL_API_KEY=...
  ANGEL_CLIENT_ID=A123456
  ANGEL_PIN=1234
  ANGEL_TOTP_SECRET=...
  ```

## Usage

### Single-stock backtest

```bash
python angel_orb_backtest.py -s RELIANCE
python angel_orb_backtest.py -s HDFCBANK --days 180 --capital 200000
python angel_orb_backtest.py -s INFY --no-filters         # baseline
python angel_orb_backtest.py -s TCS --skip-days Friday Thursday
```

### Bulk backtest — 15 stocks (recommended)

```bash
# Baseline first — raw performance with no filters
python bulk_backtest.py --no-filters --days 180

# Then with all filters
python bulk_backtest.py --days 180

# Regenerate report without re-fetching data
python bulk_backtest.py --report-only
```

Outputs:
- `bulk_report.md` — comprehensive markdown report with leaderboard, verdicts, per-stock details
- `bulk_report.csv` — leaderboard as CSV
- `trades_<SYMBOL>.csv` — per-stock trade log
- `cache/*.parquet` — cached candle data (re-runs are instant)

## Strategy rules

- **Opening range:** high/low of first 30 min (9:15–9:45 IST), 5-min candles
- **Entry:** long at next bar's open after a 5-min candle CLOSES above OR-high
- **Stop loss:** OR-low
- **Target:** entry + 1.5 × (entry − OR-low). Fixed. No trailing.
- **Square-off:** 15:15 IST (intraday MIS)
- **One trade per symbol per day.** No re-entries.
- **Skip days** with OR width < 0.20% of open (noise filter)

### Optional filters (all on by default)

| Filter | Default | Purpose |
|---|---|---|
| Breakout strength | ≥0.15% above OR-high | Reject fakeouts |
| Volume confirmation | ≥1.5× avg OR-candle volume | Confirm real breakout |
| Day-of-week | Skip Friday | Pre-weekend positioning distortion |
| Nifty alignment | Nifty also above its OR-high | Trade with the index |

## Rate limits handled

`bulk_backtest.py` respects Angel SmartAPI rate caps:
- 2.5 req/sec (cap: 3/sec)
- 170 req/min (cap: 180/min)
- Auto-retry with exponential backoff on 403
- Nifty data fetched once and shared across all 15 stocks
- All fetches cached to parquet — re-runs are instant

## Workflow

1. Run `bulk_backtest.py --no-filters --days 180` to see baseline performance across 15 stocks
2. Read `bulk_report.md` — look for stocks marked `✓ CANDIDATE`
3. Run again with filters (`bulk_backtest.py --days 180`) and compare
4. Top candidates → paper-trade for 2–4 weeks before risking real capital

## Default stock universe

15 large-cap NSE stocks with high intraday liquidity:

| Sector | Symbols |
|---|---|
| Banking & financials | HDFCBANK, ICICIBANK, SBIN, AXISBANK, BAJFINANCE |
| IT | INFY, TCS |
| Energy / conglomerate | RELIANCE |
| Capex / infra | LT, ADANIPORTS |
| Auto | MARUTI, TATAMOTORS |
| Metals | HINDALCO |
| Consumer | TITAN |
| Pharma | DRREDDY |

## Disclaimer

Algorithmic trading involves real financial risk. Past performance does not guarantee future results. Always:

- Paper-trade before going live
- Start with capital you can afford to lose entirely
- Verify SEBI compliance for retail algo trading

This is not financial advice. The author assumes no responsibility for trading losses.

## License

MIT
