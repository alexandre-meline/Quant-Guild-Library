"""
Fetch 2 years of daily OHLCV from IBKR for 3 random US equities per GICS sector,
compute daily simple returns, and save a long (stacked) composite DataFrame.

Requires TWS or IB Gateway on 127.0.0.1 with API enabled (default paper port 7497).
"""

from __future__ import annotations

import random
import time
from pathlib import Path
from threading import Thread

import pandas as pd
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper

# GICS sector -> liquid US common stocks (SMART/USD). Three are sampled per sector.
SECTOR_TICKERS: dict[str, list[str]] = {
    "Communication Services": [
        "GOOGL",
        "META",
        "DIS",
        "NFLX",
        "CMCSA",
        "T",
        "VZ",
        "TMUS",
        "CHTR",
    ],
    "Consumer Discretionary": [
        "AMZN",
        "TSLA",
        "HD",
        "MCD",
        "NKE",
        "SBUX",
        "LOW",
        "TJX",
        "BKNG",
    ],
    "Consumer Staples": [
        "PG",
        "KO",
        "PEP",
        "WMT",
        "COST",
        "MDLZ",
        "MO",
        "PM",
        "CL",
    ],
    "Energy": [
        "XOM",
        "CVX",
        "COP",
        "SLB",
        "EOG",
        "MPC",
        "PSX",
        "VLO",
        "OXY",
    ],
    "Financials": [
        "JPM",
        "BAC",
        "WFC",
        "GS",
        "MS",
        "BLK",
        "SCHW",
        "AXP",
        "C",
    ],
    "Health Care": [
        "JNJ",
        "UNH",
        "LLY",
        "ABBV",
        "MRK",
        "PFE",
        "TMO",
        "DHR",
        "BMY",
    ],
    "Industrials": [
        "CAT",
        "DE",
        "UPS",
        "HON",
        "RTX",
        "LMT",
        "GE",
        "MMM",
        "BA",
    ],
    "Information Technology": [
        "AAPL",
        "MSFT",
        "NVDA",
        "AVGO",
        "CRM",
        "AMD",
        "ORCL",
        "ADBE",
        "CSCO",
    ],
    "Materials": [
        "LIN",
        "APD",
        "SHW",
        "ECL",
        "NEM",
        "FCX",
        "DD",
        "DOW",
        "NUE",
    ],
    "Real Estate": [
        "PLD",
        "AMT",
        "EQIX",
        "SPG",
        "PSA",
        "WELL",
        "DLR",
        "O",
        "VICI",
    ],
    "Utilities": [
        "NEE",
        "DUK",
        "SO",
        "D",
        "AEP",
        "SRE",
        "EXC",
        "XEL",
        "ED",
    ],
}

HOST = "127.0.0.1"
PORT = 7497
CLIENT_ID = 123
RANDOM_SEED = 42
REQUEST_TIMEOUT_S = 90
PACING_SLEEP_S = 2.0


def _parse_bar_date(s: str) -> pd.Timestamp:
    if isinstance(s, str) and len(s) == 8 and s.isdigit():
        return pd.Timestamp(s)
    return pd.to_datetime(s)


def run_loop(app: EClient) -> None:
    app.run()


class IBKRHistoricalApp(EWrapper, EClient):
    def __init__(self) -> None:
        EClient.__init__(self, self)
        self.data: list[dict] = []
        self.finished = False
        self._errors: list[tuple[int, str]] = []

    def historicalData(self, reqId, bar) -> None:
        self.data.append(
            {
                "date": _parse_bar_date(bar.date),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            }
        )

    def historicalDataEnd(self, reqId, start, end) -> None:
        self.finished = True

    def error(self, reqId, errorCode, errorString, advancedOrderReject="") -> None:
        # Market data farm connection messages (informational)
        if errorCode in (2104, 2106, 2158, 2119):
            return
        self._errors.append((errorCode, errorString))
        if reqId >= 0:
            self.finished = True


def stock_contract(symbol: str) -> Contract:
    c = Contract()
    c.symbol = symbol
    c.secType = "STK"
    c.exchange = "SMART"
    c.currency = "USD"
    return c


def fetch_daily_bars(
    app: IBKRHistoricalApp,
    req_id: int,
    symbol: str,
    duration_str: str = "2 Y",
) -> pd.DataFrame:
    app.data = []
    app.finished = False
    app._errors.clear()
    end_time = time.strftime("%Y%m%d %H:%M:%S")
    app.reqHistoricalData(
        reqId=req_id,
        contract=stock_contract(symbol),
        endDateTime=end_time,
        durationStr=duration_str,
        barSizeSetting="1 day",
        whatToShow="TRADES",
        useRTH=1,
        formatDate=1,
        keepUpToDate=False,
        chartOptions=[],
    )
    deadline = time.time() + REQUEST_TIMEOUT_S
    while not app.finished and time.time() < deadline:
        time.sleep(0.25)
    if not app.finished:
        try:
            app.cancelHistoricalData(req_id)
        except Exception:
            pass
        return pd.DataFrame()
    if app._errors and not app.data:
        return pd.DataFrame()
    df = pd.DataFrame(app.data)
    if df.empty:
        return df
    df = df.sort_values("date").drop_duplicates(subset=["date"])
    df["daily_return"] = df["close"].pct_change()
    df = df.dropna(subset=["daily_return"])
    df.insert(0, "symbol", symbol)
    return df.reset_index(drop=True)


def sample_tickers_per_sector(
    seed: int | None = RANDOM_SEED,
    n: int = 3,
) -> list[tuple[str, str]]:
    rng = random.Random(seed)
    pairs: list[tuple[str, str]] = []
    for sector, tickers in SECTOR_TICKERS.items():
        pool = list(tickers)
        k = min(n, len(pool))
        chosen = rng.sample(pool, k=k)
        for sym in chosen:
            pairs.append((sector, sym))
    return pairs


def _progress_bar(current: int, total: int, width: int = 28) -> str:
    if total <= 0:
        return "[" + "-" * width + "]"
    filled = min(width, int(round(width * current / total)))
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def build_stacked_returns_dataframe(pairs: list[tuple[str, str]]) -> pd.DataFrame:
    app = IBKRHistoricalApp()
    app.connect(HOST, PORT, CLIENT_ID)
    thread = Thread(target=run_loop, args=(app,), daemon=True)
    thread.start()
    time.sleep(1)

    rows: list[pd.DataFrame] = []
    req_id = 1
    total = len(pairs)
    try:
        for i, (sector, symbol) in enumerate(pairs, start=1):
            bar = _progress_bar(i - 1, total)
            print(
                f"{bar} {i - 1}/{total}  {sector} | {symbol}  downloading…",
                flush=True,
            )
            t0 = time.perf_counter()
            sub = fetch_daily_bars(app, req_id, symbol)
            elapsed = time.perf_counter() - t0
            req_id += 1
            bar = _progress_bar(i, total)
            if sub.empty:
                err = app._errors[-1] if app._errors else None
                print(
                    f"{bar} {i}/{total}  {sector} | {symbol}  skipped  "
                    f"({elapsed:.1f}s){f'  {err}' if err else ''}",
                    flush=True,
                )
                time.sleep(PACING_SLEEP_S)
                continue
            sub.insert(0, "sector", sector)
            rows.append(sub)
            n = len(sub)
            print(
                f"{bar} {i}/{total}  {sector} | {symbol}  {n} return rows  ({elapsed:.1f}s)",
                flush=True,
            )
            time.sleep(PACING_SLEEP_S)
    finally:
        app.disconnect()

    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out = out.sort_values(["sector", "symbol", "date"]).reset_index(drop=True)
    return out


def main() -> pd.DataFrame:
    out_dir = Path(__file__).resolve().parent
    pairs = sample_tickers_per_sector()
    print(
        f"Downloading {len(pairs)} symbols (2Y daily, ~{PACING_SLEEP_S:.0f}s pacing between requests)…\n",
        flush=True,
    )
    df = build_stacked_returns_dataframe(pairs)
    if df.empty:
        print("No data retrieved; check TWS/Gateway and market data subscriptions.")
        return df

    pkl_path = out_dir / "efficient_frontier_daily_returns.pkl"
    csv_path = out_dir / "efficient_frontier_daily_returns.csv"
    df.to_pickle(pkl_path)
    df.to_csv(csv_path, index=False)
    print(f"Wrote {len(df)} rows -> {pkl_path.name}, {csv_path.name}")
    return df


if __name__ == "__main__":
    main()
