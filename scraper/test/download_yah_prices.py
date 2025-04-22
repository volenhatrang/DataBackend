import requests
import yfinance as yf
import pandas as pd
import re
import warnings
from datetime import datetime
from tabulate import tabulate

warnings.filterwarnings("ignore", category=DeprecationWarning)


def sys_time():
    """Return the current system time as a string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def update_updated(df):
    """Update the 'updated' column to the current time."""
    df = df.copy()
    df["updated"] = sys_time()
    return df


def view_data(df):
    if len(df) > 10:
        df_display = df.head(5)._append(df.tail(5))
    else:
        df_display = df
    print()
    print(tabulate(df_display, headers="keys", tablefmt="pretty", showindex=False))


def clean_colnames(df):
    """
    Cleans column names of a DataFrame by:
    - Converting to lowercase
    - Replacing spaces and hyphens with underscores
    - Replacing '%' with 'percent_'
    - Removing '*' characters
    - Stripping any leading/trailing whitespace

    Parameters:
    df (pd.DataFrame): The DataFrame with columns to clean.

    Returns:
    pd.DataFrame: A new DataFrame with cleaned column names.
    """
    df = df.copy()
    df.columns = [
        re.sub(
            r"\s+",
            "",
            re.sub(
                r"\-",
                "_",
                re.sub(
                    r"\%",
                    "percent_",
                    re.sub(r"\*", "", col.lower()),
                ),
            ),
        )  # Remove '*' and make lowercase
        for col in df.columns
    ]
    return df


def download_yah_prices_by_api(
    pCodesource="AAPL", pInterval="5m", pNbdays=60, Hour_adjust=0
):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }
    my_url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{pCodesource}"
        f"?region=US&lang=en-US&includePrePost=false&interval={pInterval}"
        f"&range={pNbdays}d&corsDomain=finance.yahoo.com&.tsrc=finance"
    )

    response = requests.get(my_url, headers=headers)
    dt_json = response.json()

    df_combined = pd.DataFrame()

    if dt_json and "chart" in dt_json and dt_json["chart"]["result"]:
        result = dt_json["chart"]["result"][0]
        meta = result.get("meta", {})

        gmtoffset = meta.get("gmtoffset", 0)
        calc_hour_adjust = gmtoffset / 3600
        if calc_hour_adjust > 0:
            Hour_adjust = calc_hour_adjust

        rCodesource = meta.get("symbol", pCodesource)
        print(f"=== {rCodesource} ===")

        timestamps = result.get("timestamp", [])
        df_timestamp = pd.DataFrame({"timestamp": timestamps})
        df_timestamp["ID"] = range(1, len(df_timestamp) + 1)

        indicators = result.get("indicators", {})
        quote = indicators.get("quote", [{}])[0]

        if len(quote.get("open", [])) > 2:
            df_open = pd.DataFrame({"open": quote.get("open", [])})
            df_open["ID"] = range(1, len(df_open) + 1)

            df_high = pd.DataFrame({"high": quote.get("high", [])})
            df_high["ID"] = range(1, len(df_high) + 1)

            df_low = pd.DataFrame({"low": quote.get("low", [])})
            df_low["ID"] = range(1, len(df_low) + 1)

            df_close = pd.DataFrame({"close": quote.get("close", [])})
            df_close["ID"] = range(1, len(df_close) + 1)

            if "adjclose" in indicators and indicators["adjclose"]:
                df_closeadj = pd.DataFrame(
                    {
                        "close_adj": indicators["adjclose"][0].get(
                            "adjclose", quote.get("close", [])
                        )
                    }
                )
            else:
                df_closeadj = pd.DataFrame({"close_adj": quote.get("close", [])})
            df_closeadj["ID"] = range(1, len(df_closeadj) + 1)

            df_volume = pd.DataFrame({"volume": quote.get("volume", [])})
            df_volume["ID"] = range(1, len(df_volume) + 1)

            df_combined = df_timestamp.merge(df_open[["ID", "open"]], on="ID")
            df_combined = df_combined.merge(df_high[["ID", "high"]], on="ID")
            df_combined = df_combined.merge(df_low[["ID", "low"]], on="ID")
            df_combined = df_combined.merge(df_close[["ID", "close"]], on="ID")
            df_combined = df_combined.merge(df_closeadj[["ID", "close_adj"]], on="ID")
            df_combined = df_combined.merge(df_volume[["ID", "volume"]], on="ID")

            df_combined["datetime"] = pd.to_datetime(
                df_combined["timestamp"], unit="s", utc=True
            )
            df_combined["datetime"] = df_combined["datetime"] + pd.to_timedelta(
                Hour_adjust, unit="h"
            )

            cols_order = [
                "datetime",
                "open",
                "high",
                "low",
                "close",
                "close_adj",
                "volume",
            ]
            df_combined = df_combined[cols_order]

            df_combined["codesource"] = rCodesource
            df_combined["source"] = "YAH"
            cols_order = ["codesource"] + [
                col for col in df_combined.columns if col != "codesource"
            ]
            df_combined = df_combined[cols_order]

            df_combined["date"] = df_combined["datetime"].dt.date

            df_combined["timestamp"] = df_combined["datetime"]

            if pInterval == "1d":
                df_combined = df_combined.sort_values("timestamp").drop_duplicates(
                    subset="date", keep="first"
                )

    df_combined = update_updated(df_combined)
    return df_combined


def download_yah_prices_by_code(ticker="AAPL", period="max"):
    try:
        period_days = {
            "1d": 1,
            "5d": 5,
            "1mo": 30,
            "3mo": 90,
            "6mo": 180,
            "1y": 365,
            "2y": 730,
            "5y": 1825,
            "10y": 3650,
            "max": 60000,
        }
        price_data = pd.DataFrame()
        if period == "max":
            price_data = download_yah_prices_by_api(
                pCodesource=ticker,
                pInterval="1d",
                pNbdays=period_days.get(period),
                Hour_adjust=0,
            )
            price_data = price_data[
                ["open", "high", "low", "close", "close_adj", "volume", "date"]
            ]
            price_data = price_data.rename(columns={"close_adj": "adjclose"})
        if price_data.empty:
            price_data = yf.download(ticker, period=period, auto_adjust=False)
            if price_data.empty:
                symbol = yf.Ticker(ticker)
                price_data = symbol.history(period=period, auto_adjust=False)[
                    ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
                ]
                if price_data.empty:
                    print(f"⚠ Warning: No data for {ticker}")
                    return pd.DataFrame()

        if isinstance(price_data.columns, pd.MultiIndex):
            price_data.columns = price_data.columns.get_level_values(0)
        price_data = price_data.loc[:, ~price_data.columns.duplicated()]

        price_data = price_data.reset_index()
        price_data["ticker"] = ticker
        price_data = clean_colnames(price_data)
        price_data["date"] = pd.to_datetime(
            price_data["date"], utc=True
        ).dt.tz_localize(None)

        price_data = update_updated(price_data)
        price_data = price_data.drop(columns=["index"], errors="ignore")

        view_data(price_data)
        return price_data
    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()


def download_prices_and_save_csv(
    ticker_list, period="max", output_file="yah_prices.csv"
):
    all_data = []

    for ticker in ticker_list:
        print(f"⬇ Downloading: {ticker}")
        df = download_yah_prices_by_code(ticker=ticker, period=period)
        if not df.empty:
            all_data.append(df)
        else:
            print(f"⚠ Skipped {ticker} due to empty data.")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(output_file, index=False)
        print(f"✅ Data saved to {output_file}")
        return final_df
    else:
        print("❌ No data downloaded.")
        return pd.DataFrame()


if __name__ == "__main__":
    LIST_CODES_FILE = "D:/WebBEQ/DataBackend/scraper/src/database/reference_data/list_code_by_source/LIST_CODESOURCE_DOWNLOAD_SPECIAL.txt"
    df = pd.read_csv(LIST_CODES_FILE, usecols=["YAH"], dtype=str)
    tickers = df["YAH"].dropna().unique().tolist()

    df = download_prices_and_save_csv(
        tickers, period="max", output_file="yah_prices.csv"
    )
