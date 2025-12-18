import json
import pandas as pd
import sys
import yahoo_finance_api
import indicators
import yfinance as yf

other_listed = "https://raw.githubusercontent.com/datasets/nyse-other-listings/refs/heads/main/data/other-listed.csv"
nasdaq_listed = "https://raw.githubusercontent.com/datasets/nasdaq-listings/refs/heads/main/data/nasdaq-listed-symbols.csv"

df = pd.read_csv(other_listed)
df2 = pd.read_csv(nasdaq_listed)

df.dropna(inplace=True)
df2.dropna(inplace=True)

# df header:
# ACT Symbol,Company Name,Security Name,Exchange,CQS Symbol,ETF,Round Lot Size,Test Issue,NASDAQ Symbol

# df2 header:
# Symbol,Company Name,Security Name,Market Category,Test Issue,Financial Status,Round Lot Size,ETF,NextShares

stocks = []

for index, row in df.iterrows():
    if row["Exchange"] not in ['A', 'N', 'P']:
        continue
    if row["ETF"] == 'Y':
        continue
    if '$' in row["ACT Symbol"]:
        continue
    if not 'common share' in row["Company Name"].lower() and not 'common stock' in row["Company Name"].lower():
        continue
    stocks.append({"ticker": row["ACT Symbol"], "name": row["Company Name"]})

for index, row in df2.iterrows():
    if row["ETF"] == 'Y':
        continue
    if row["Financial Status"] != 'N':
        continue
    if row["Test Issue"] == 'Y':
        continue
    stocks.append({"ticker": row["Symbol"], "name": row["Company Name"]})
    

if '-l' in sys.argv or '--list-tickers' in sys.argv:
    for stock in stocks:
        print(stock["ticker"], stock["name"])
    exit(0)

watchlist=[]

if '-t' in sys.argv or '--test' in sys.argv:
    stocks = [{"ticker":sys.argv[sys.argv.index('-t')+1] if '-t' in sys.argv else sys.argv[sys.argv.index('--test')+1],"name":"Dummy"}]

header = [
    "ticker",
    "name",
    "rsi",
    "priceToBook",
    "enterpriseToRevenue",
    "enterpriseToEbitda",
    "targetHighPrice",
    "targetLowPrice",
    "targetMeanPrice",
    "targetMedianPrice",
    "recommendationMean",
    "recommendationKey",
    "totalCashPerShare",
    "quickRatio",
    "currentRatio",
    "debtToEquity",
    "returnOnAssets",
    "returnOnEquity",
    "earningsGrowth",
    "revenueGrowth",
    "grossMargins",
    "operatingMargins",
    "averageAnalystRating",
    "trailingPegRatio"
    ]

for stock in stocks:
    print(stock["ticker"], stock["name"])
    try:
        df = yahoo_finance_api.retrieve_daily_prices(stock["ticker"],period=365)
    except Exception as e:
        print(f"Error retrieving daily prices for {stock['ticker']}: {e}")
        continue
    if df.empty:
        continue
    df = indicators.rsi(df)

    stock["rsi"] = df.loc[df.index[-1], "RSI"]

    # skip if less than 200 days of data
    if len(df) < 200:
        print(f"skipped {stock['ticker']}: less than 300 days of data")
        continue

    # skip if last price is less than $5
    if float(df.loc[df.index[-1], "Close"]) < 5:
        print(f"skipped {stock['ticker']}: price less than $5")
        continue

    # condition to watchlist
    if stock["rsi"] > 70 or stock["rsi"] < 30:
        # get additional information
        try:
            info = yf.Ticker(stock["ticker"]).info
        except Exception as e:
            print(f"Error retrieving info for {stock['ticker']}: {e}")
            continue
        for key in header:
            if not key in stock:
                if key in info:
                    stock[key] = info[key]
                else:
                    stock[key] = None

        # skip SPAC
        if 'ipoExpectedDate' in info and info['ipoExpectedDate'] is not None:
            print(f"skipped {stock['ticker']}: it's a SPAC")
            continue

        if stock["rsi"] > 70:
            stock["action"] = "short"
        if stock["rsi"] < 30:
            stock["action"] = "long"

        # TODO: add other info
        watchlist.append(stock)

# sort watchlist by ticker
watchlist.sort(key=lambda x: x["ticker"])

# save long and short to data/report.json
try:
    with open("data/report.json", "r") as f:
        old_report = json.load(f)
except Exception as e:
    old_report = None

if watchlist != old_report:
    print("Report has changed")
    with open("data/report.json", "w") as f:
        json.dump(watchlist, f)
else:
    print("Report has not changed")

# add sorting value
for index, stock in enumerate(watchlist):
    stock["sorting"] = 0
    if stock["action"] == "long":
        if stock.get("trailingPE") is not None and stock["trailingPE"] > 0 and stock["trailingPE"] < 30:
            stock["sorting"] = stock["sorting"] - 30 + stock["trailingPE"]
        else:
            stock["sorting"] = stock["sorting"] + 20
        if stock.get("forwardPE") is not None and stock["forwardPE"] > 0 and stock["forwardPE"] < 30:
            stock["sorting"] = stock["sorting"] - 30 + stock["forwardPE"]
        else:
            stock["sorting"] = stock["sorting"] + 20
        if stock.get("trailingPegRatio") is not None and stock["trailingPegRatio"] > 0 and stock["trailingPegRatio"] < 2:
            stock["sorting"] = stock["sorting"] - (1/stock["trailingPegRatio"]*30)
        else:
            stock["sorting"] = stock["sorting"] + 20
        stock["sorting"] = stock["sorting"] - 30 + stock["rsi"]
        if stock.get("priceToBook") is not None and stock["priceToBook"] < 2:
            stock["sorting"] = stock["sorting"] - 10
        if stock.get("quickRatio") is not None and stock["quickRatio"] > 1:
            stock["sorting"] = stock["sorting"] - 5
        if stock.get("currentRatio") is not None and stock["currentRatio"] > 1:
            stock["sorting"] = stock["sorting"] - 5
    if stock["action"] == "short":
        if stock.get("trailingPE") is not None and stock["trailingPE"] > 0 and stock["trailingPE"] < 30:
            stock["sorting"] = stock["sorting"] + 30 - stock["trailingPE"]
        else:
            stock["sorting"] = stock["sorting"] + 20
        if stock.get("forwardPE") is not None and stock["forwardPE"] > 0 and stock["forwardPE"] < 30:
            stock["sorting"] = stock["sorting"] + 30 - stock["forwardPE"]
        else:
            stock["sorting"] = stock["sorting"] + 20
        if stock.get("trailingPegRatio") is not None and stock["trailingPegRatio"] > 0 and stock["trailingPegRatio"] < 2:
            stock["sorting"] = stock["sorting"] + (1/stock["trailingPegRatio"]*30)
        else:
            stock["sorting"] = stock["sorting"] + 20
        stock["sorting"] = stock["sorting"] + 100 - stock["rsi"]
        if stock.get("priceToBook") is not None and stock["priceToBook"] < 2:
            stock["sorting"] = stock["sorting"] + 30
        if stock.get("quickRatio") is not None and stock["quickRatio"] < 1:
            stock["sorting"] = stock["sorting"] - 15
        if stock.get("currentRatio") is not None and stock["currentRatio"] < 1:
            stock["sorting"] = stock["sorting"] - 15
    watchlist[index] = stock
# sort watchlist by sorting
watchlist.sort(key=lambda x: x["sorting"])

# Transform watchlist to DataFrame
df_watchlist = pd.DataFrame(watchlist)

# Ensure all header columns exist
for col in header:
    if col not in df_watchlist.columns:
        df_watchlist[col] = None

# add current date in gg/mm/yyyy hh:mm format to the "Ticker" header
from datetime import datetime
now = datetime.now()
new_ticker_header = "Ticker "+now.strftime("%d/%m/%Y %H:%M")
df_watchlist.rename(columns={"ticker": new_ticker_header}, inplace=True)
header[0] = new_ticker_header

# write data/long.csv and data/short.csv
if "action" in df_watchlist.columns:
    df_watchlist[df_watchlist["action"] == "long"].to_csv("data/long.csv", columns=header, index=False)
    df_watchlist[df_watchlist["action"] == "short"].to_csv("data/short.csv", columns=header, index=False)
else:
    pd.DataFrame(columns=header).to_csv("data/long.csv", index=False)
    pd.DataFrame(columns=header).to_csv("data/short.csv", index=False)
