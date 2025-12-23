import pandas as pd

OTHER_LISTED_URL = "https://raw.githubusercontent.com/datasets/nyse-other-listings/refs/heads/main/data/other-listed.csv"
NASDAQ_LISTED_URL = "https://raw.githubusercontent.com/datasets/nasdaq-listings/refs/heads/main/data/nasdaq-listed-symbols.csv"


def load_us_listed_common_stocks(
    other_listed_url: str = OTHER_LISTED_URL,
    nasdaq_listed_url: str = NASDAQ_LISTED_URL,
) -> list[dict[str, str]]:
    df_other = pd.read_csv(other_listed_url)
    df_nasdaq = pd.read_csv(nasdaq_listed_url)

    df_other.dropna(inplace=True)
    df_nasdaq.dropna(inplace=True)

    stocks: list[dict[str, str]] = []

    for _, row in df_other.iterrows():
        if row["Exchange"] not in ["A", "N", "P"]:
            continue
        if row["ETF"] == "Y":
            continue
        if "$" in row["ACT Symbol"]:
            continue
        company_name = str(row["Company Name"])  # defensive
        company_name_l = company_name.lower()
        if "common share" not in company_name_l and "common stock" not in company_name_l:
            continue
        stocks.append({"ticker": row["ACT Symbol"], "name": company_name})

    for _, row in df_nasdaq.iterrows():
        if row["ETF"] == "Y":
            continue
        if row["Financial Status"] != "N":
            continue
        if row["Test Issue"] == "Y":
            continue
        stocks.append({"ticker": row["Symbol"], "name": row["Company Name"]})

    return stocks
