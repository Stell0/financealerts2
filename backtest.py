import sys
from datetime import datetime, timedelta

import pandas as pd

import indicators
import universe
import yahoo_finance_api


HORIZONS_DAYS = [10, 20, 30, 60]
DROP_THRESHOLDS = [0.10, 0.20, 0.30, 0.50]


def _get_arg_value(flag: str) -> str | None:
	if flag not in sys.argv:
		return None
	idx = sys.argv.index(flag)
	if idx + 1 >= len(sys.argv):
		return None
	return sys.argv[idx + 1]


def _format_pct(numerator: int, denominator: int) -> str:
	if denominator <= 0:
		return "n/a"
	return f"{(numerator / denominator) * 100:.2f}%"


def main() -> int:
	stocks = universe.load_us_listed_common_stocks()

	if "-l" in sys.argv or "--list-tickers" in sys.argv:
		for stock in stocks:
			print(stock["ticker"], stock["name"])
		return 0

	test_ticker = None
	if "-t" in sys.argv:
		test_ticker = _get_arg_value("-t")
	elif "--test" in sys.argv:
		test_ticker = _get_arg_value("--test")
	if test_ticker:
		stocks = [{"ticker": test_ticker, "name": "Dummy"}]

	# Use multiple years by default (cache no longer truncates).
	start = datetime.today() - timedelta(days=365 * 5)

	# stats[signal][horizon][drop] = {"hits": int, "events": int, "skipped": int}
	stats: dict[str, dict[int, dict[float, dict[str, int]]]] = {}
	for signal in ["overbought", "oversold"]:
		stats[signal] = {}
		for horizon in HORIZONS_DAYS:
			stats[signal][horizon] = {}
			for drop in DROP_THRESHOLDS:
				stats[signal][horizon][drop] = {"hits": 0, "events": 0, "skipped": 0}

	skipped_no_next_day = 0
	processed = 0

	for stock in stocks:
		ticker = stock["ticker"]
		processed += 1
		print(f"{processed}/{len(stocks)} {ticker}")

		try:
			df = yahoo_finance_api.retrieve_daily_prices(ticker, start=start)
		except Exception as e:
			print(f"Error retrieving daily prices for {ticker}: {e}")
			continue

		if df is None or df.empty:
			continue

		df = df.sort_index()
		if "Close" not in df.columns:
			continue

		# keep consistent with main.py guards
		if len(df) < 200:
			continue
		try:
			if float(df["Close"].iloc[-1]) < 5:
				continue
		except Exception:
			continue

		df = indicators.rsi(df)
		if "RSI" not in df.columns:
			continue

		rsi = df["RSI"]
		prev = rsi.shift(1)

		overbought_cross = (prev <= 70) & (rsi > 70)
		oversold_cross = (prev >= 30) & (rsi < 30)

		close = df["Close"].astype(float)

		# Use integer positions for fast forward slicing
		idx_overbought = list(overbought_cross[overbought_cross.fillna(False)].index)
		idx_oversold = list(oversold_cross[oversold_cross.fillna(False)].index)

		index_to_pos = {ts: i for i, ts in enumerate(df.index)}

		def process_events(event_index_list: list[pd.Timestamp], signal: str) -> None:
			nonlocal skipped_no_next_day

			for ts in event_index_list:
				i = index_to_pos.get(ts)
				if i is None:
					continue
				ref_i = i + 1
				if ref_i >= len(df):
					skipped_no_next_day += 1
					continue
				ref_close = float(close.iloc[ref_i])
				if ref_close <= 0:
					continue

				for horizon in HORIZONS_DAYS:
					end_i = ref_i + horizon
					if end_i > len(df):
						for drop in DROP_THRESHOLDS:
							stats[signal][horizon][drop]["skipped"] += 1
						continue

					window = close.iloc[ref_i:end_i]
					if window.empty:
						for drop in DROP_THRESHOLDS:
							stats[signal][horizon][drop]["skipped"] += 1
						continue

					min_close = float(window.min())
					drop_pct = (min_close - ref_close) / ref_close

					for drop in DROP_THRESHOLDS:
						stats[signal][horizon][drop]["events"] += 1
						if drop_pct <= -drop:
							stats[signal][horizon][drop]["hits"] += 1

		process_events(idx_overbought, "overbought")
		process_events(idx_oversold, "oversold")

	print("\n=== RSI Cross Backtest (entry = next-day close; forward window starts that day; close-only) ===")
	print(f"Skipped events with no next day: {skipped_no_next_day}")

	for signal in ["overbought", "oversold"]:
		print(f"\n--- {signal.upper()} ---")
		for horizon in HORIZONS_DAYS:
			parts = [f"{horizon}d"]
			denom = stats[signal][horizon][DROP_THRESHOLDS[0]]["events"]
			parts.append(f"events={denom}")
			for drop in DROP_THRESHOLDS:
				hits = stats[signal][horizon][drop]["hits"]
				events = stats[signal][horizon][drop]["events"]
				parts.append(f">={int(drop*100)}%: {_format_pct(hits, events)}")
			print("  " + ", ".join(parts))

	return 0


if __name__ == "__main__":
	raise SystemExit(main())
