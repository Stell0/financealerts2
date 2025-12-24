import pandas as pd
import numpy as np
import talib as ta
import os

# set variables from environment
RSI_timeperiod = int(os.environ.get("RSI_timeperiod",14))
SMA_timeperiod = int(os.environ.get("SMA_timeperiod",200))
LINEARREG_timeperiod = int(os.environ.get("LINEARREG_timeperiod",10))

def rsi(df,timeperiod=14,label="RSI",):
    df[label] = ta.RSI(df['Close'].to_numpy().flatten(), timeperiod=timeperiod)
    return df

def sma(df,timeperiod=200,label="SMA"):
    df[label] = ta.SMA(df['Close'].to_numpy().flatten(), timeperiod=timeperiod)
    return df 

def linearreg(df,timeperiod=10):
    df['LINEARREG'] = ta.LINEARREG(df['Close'].to_numpy().flatten(), timeperiod=timeperiod) # b+m*(period-1)
    df['LINEARREG_SLOPE'] = ta.LINEARREG_SLOPE(df['Close'].to_numpy().flatten(), timeperiod=timeperiod) # m
    df['LINEARREG_INTERCEPT'] = ta.LINEARREG_INTERCEPT(df['Close'].to_numpy().flatten(), timeperiod=timeperiod) # b
    df['LRFORECAST'] = df['LINEARREG_INTERCEPT'] + df['LINEARREG_SLOPE'] * timeperiod
    return df

def macd(df,fastperiod=12,slowperiod=26,signalperiod=9):
	macd, macdsignal, macdhist = ta.MACD(df['Close'].to_numpy().flatten(), fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod)
	df['MACD'] = macd
	df['MACD_SIGNAL'] = macdsignal
	df['MACD_HIST'] = macdhist
	return df

def bb(df,timeperiod=20,nbdevup=2,nbdevdn=2,matype=0):
	upperband, middleband, lowerband = ta.BBANDS(df['Close'].to_numpy().flatten(), timeperiod=timeperiod, nbdevup=nbdevup, nbdevdn=nbdevdn, matype=matype)
	df['BB_UPPER'] = upperband
	df['BB_MIDDLE'] = middleband
	df['BB_LOWER'] = lowerband
	return df


def log_price_change(df: pd.DataFrame, price_col: str = "Close", label: str = "LogPriceChange") -> pd.DataFrame:
	"""Add logarithmic price change (log return) column.

	Definition: r_t = ln(p_t / p_{t-1}).
	Values are NaN when current/previous price is missing or non-positive.
	"""
	if price_col not in df.columns:
		return df

	out = df.copy()
	price = pd.to_numeric(out[price_col], errors="coerce").astype(float)
	prev = price.shift(1)

	valid = (price > 0) & (prev > 0)
	out[label] = np.nan
	out.loc[valid, label] = np.log(price[valid] / prev[valid])
	return out


def normalize_series(
	series: pd.Series,
	method: str = "zscore",
	*,
	feature_range: tuple[float, float] = (0.0, 1.0),
	ddof: int = 0,
	eps: float = 1e-12,
) -> pd.Series:
	"""Normalize a pandas Series using a chosen method.

	Supported methods:
	- "zscore": (x - mean) / std
	- "minmax": scale to feature_range (default [0, 1])
	- "robust": (x - median) / IQR
	- "maxabs": x / max(abs(x))
	- "l2": x / sqrt(sum(x^2))

	Non-numeric values are coerced to NaN. NaNs stay NaN.
	"""
	if series is None:
		raise ValueError("series must not be None")

	s = pd.to_numeric(series, errors="coerce").astype(float)
	method_norm = (method or "").strip().lower()

	if method_norm in ("z", "zscore", "standard", "standardize"):
		mu = float(s.mean(skipna=True))
		sigma = float(s.std(skipna=True, ddof=ddof))
		if not np.isfinite(sigma) or abs(sigma) <= eps:
			return s * np.nan
		return (s - mu) / sigma

	if method_norm in ("minmax", "min-max", "range"):
		lo, hi = feature_range
		if hi <= lo:
			raise ValueError("feature_range must be (low, high) with high > low")
		min_v = float(s.min(skipna=True))
		max_v = float(s.max(skipna=True))
		denom = max_v - min_v
		if not np.isfinite(denom) or abs(denom) <= eps:
			return s * np.nan
		scaled01 = (s - min_v) / denom
		return scaled01 * (hi - lo) + lo

	if method_norm in ("robust", "iqr"):
		med = float(s.median(skipna=True))
		q1 = float(s.quantile(0.25, interpolation="linear"))
		q3 = float(s.quantile(0.75, interpolation="linear"))
		iqr = q3 - q1
		if not np.isfinite(iqr) or abs(iqr) <= eps:
			return s * np.nan
		return (s - med) / iqr

	if method_norm in ("maxabs", "max-abs"):
		m = float(np.nanmax(np.abs(s.to_numpy())))
		if not np.isfinite(m) or m <= eps:
			return s * np.nan
		return s / m

	if method_norm in ("l2", "unit", "unitnorm", "unit-norm"):
		arr = s.to_numpy(dtype=float)
		norm = float(np.sqrt(np.nansum(arr * arr)))
		if not np.isfinite(norm) or norm <= eps:
			return s * np.nan
		return s / norm

	raise ValueError(f"Unknown normalization method: {method!r}")


def normalize_column(
	df: pd.DataFrame,
	col: str,
	label: str | None = None,
	method: str = "zscore",
	*,
	feature_range: tuple[float, float] = (0.0, 1.0),
	ddof: int = 0,
	eps: float = 1e-12,
) -> pd.DataFrame:
	"""Normalize a DataFrame column and store it as a new column.

	This is a thin wrapper around normalize_series() for pipeline-friendly usage.
	"""
	if col not in df.columns:
		return df
	if label is None:
		label = f"{col}_Normalized"
	out = df.copy()
	out[label] = normalize_series(
		out[col],
		method=method,
		feature_range=feature_range,
		ddof=ddof,
		eps=eps,
	)
	return out