import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta

def _sanitize_ticker_filename(ticker):
    """Sanitize ticker name for use as filename"""
    return ticker.replace('/', '_').replace('\\', '_').replace(':', '_').replace('*', '_').replace('?', '_').replace('"', '_').replace('<', '_').replace('>', '_').replace('|', '_')

def retrieve_daily_prices(ticker, period = None, start=None, end=None):
    # Sanitize ticker for filename
    safe_ticker = _sanitize_ticker_filename(ticker)
    cache_file = f'data/{safe_ticker}.csv'

    def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df

    def _apply_window(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        if start is not None or end is not None:
            start_ts = pd.to_datetime(start) if start is not None else None
            end_ts = pd.to_datetime(end) if end is not None else None
            return df.loc[start_ts:end_ts]
        if isinstance(period, int):
            return df.tail(period)
        return df

    # Check for existing cache
    if os.path.exists(cache_file):
        try:
            # Load cached data
            cached_df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            
            # Check if it is the bad format (MultiIndex header saved) or valid data
            if not cached_df.empty:
                # If index contains "Ticker" or strings that shouldn't be dates, it might be the bad format
                # However, with parse_dates=True, if it failed to parse, the index might be object.
                # If it parsed successfully, index should be DatetimeIndex.
                if not isinstance(cached_df.index, pd.DatetimeIndex):
                    # Attempt to handle bad format or just discard
                    print(f"Warning: Cache for {ticker} seems corrupted or in old format. Ignoring.")
                else:
                    if not cached_df.empty:
                        cached_df = cached_df.sort_index()

                        overlap_days = 5
                        last_ts = cached_df.index[-1]
                        fetch_start = (last_ts.to_pydatetime() - timedelta(days=overlap_days))

                        if end is None:
                            fetch_end = datetime.today() + timedelta(days=1)
                        else:
                            fetch_end = pd.to_datetime(end).to_pydatetime() + timedelta(days=1)

                        new_df = yf.download(
                            ticker,
                            start=fetch_start,
                            end=fetch_end,
                            progress=False,
                            auto_adjust=True,
                            timeout=30,
                        )
                        new_df = _flatten_columns(new_df)

                        if not new_df.empty:
                            combined_df = pd.concat([cached_df, new_df])
                            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                            combined_df = combined_df.sort_index()
                            combined_df.to_csv(cache_file)
                            return _apply_window(combined_df)

                        return _apply_window(cached_df)

        except Exception as e:
            print(f"Warning: Could not load cache for {ticker}: {e}. Fetching fresh data.")
            # Fall through to fetch fresh data

    # Fallback / No Cache logic
    # Default when not specified: fetch one year, but do NOT truncate what gets cached.
    if start is None and period is None:
        start = datetime.today() - timedelta(days=365)

    if end is None:
        end_fetch = datetime.today() + timedelta(days=1)
    else:
        end_fetch = pd.to_datetime(end).to_pydatetime() + timedelta(days=1)

    df = yf.download(
        ticker,
        start=start,
        end=end_fetch,
        period=period,
        progress=False,
        auto_adjust=True,
        timeout=30,
    )
    df = _flatten_columns(df)

    if not df.empty:
        df = df.sort_index()
        df.to_csv(cache_file)

    return _apply_window(df)

def retrieve_hourly_prices(ticker, period = None, start=None, end=None):
    # set period to 14 days if not specified
    if period is None:
        period = 14

    # set start to today - period if not specified
    if start is None:
        start = datetime.today() - timedelta(days=period)

    # set end to today if not specified
    if end is None:
        end = datetime.today()

    df = yf.download(ticker, start, end, interval='1h',progress=False)
    return df

def get_next_earnings_date(ticker):
    stock = yf.Ticker(ticker)
    return stock.earnings_dates
