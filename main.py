import json
import pandas as pd
import sys
import yahoo_finance_api
import indicators

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

long=[]
short=[]

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
    if stock["rsi"] > 70:
        short.append(stock)
    if stock["rsi"] < 30:
        long.append(stock)

print("Long:")
for stock in long:
    print(f"{stock['ticker']} - {stock['name']} - RSI:{stock['rsi']}")
print("Short:")
for stock in short:
    print(f"{stock['ticker']} - {stock['name']} - RSI:{stock['rsi']}")

try:
    with open("data/report.json", "r") as f:
        old_report = json.load(f)
except Exception as e:
    old_report = None

report = {
    "long": long,
    "short": short
}

if report != old_report:
    print("Report has changed")
    with open("data/report.json", "w") as f:
        json.dump(report, f)
else:
    print("Report has not changed")

with open("data/long.csv", "w") as f:
    f.write("Ticker,Name,RSI\n")
    for stock in long:
        f.write(f"{stock['ticker']},\"{stock['name']}\",{stock['rsi']}\n")

with open("data/short.csv", "w") as f:
    f.write("Ticker,Name,RSI\n")
    for stock in short:
        f.write(f"{stock['ticker']},\"{stock['name']}\",{stock['rsi']}\n")
