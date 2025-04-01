import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))

from common_fn import *


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
    # view_data(df_combined)
    return df_combined


def standardize_datetime(
    df, date_col="date", target_tz="UTC", date_format="%Y-%m-%d %H:%M:%S"
):
    """
    Đồng bộ cột ngày tháng từ các nguồn dữ liệu Yahoo Finance về một kiểu datetime64[ns, UTC] hoặc định dạng cố định.

    Parameters:
    - df (pd.DataFrame): DataFrame chứa dữ liệu.
    - date_col (str): Tên cột ngày tháng cần đồng bộ.
    - target_tz (str): Múi giờ cần chuyển đổi (mặc định: UTC).
    - date_format (str or None): Nếu None, giữ nguyên datetime64[ns, UTC]. Nếu có format, chuyển về dạng chuỗi.

    Returns:
    - pd.DataFrame: DataFrame đã đồng bộ cột `date`.
    """

    if date_col not in df.columns:
        raise ValueError(f"Cột '{date_col}' không tồn tại trong DataFrame")

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce", utc=True)

    if df[date_col].dt.tz is not None:
        df[date_col] = df[date_col].dt.tz_convert(target_tz).dt.tz_localize(None)

    if date_format:
        df[date_col] = df[date_col].dt.strftime(date_format)

    return df


def standardize_date(date_value):
    """
    Chuẩn hóa giá trị ngày thành YYYY-MM-DD, xử lý kiểu object (chuỗi).
    """
    # Nếu là chuỗi (object)
    if isinstance(date_value, str):
        # Loại bỏ khoảng trắng và kiểm tra định dạng
        date_value = date_value.strip()
        parts = date_value.split("-")
        if len(parts) == 3 and all(part.isdigit() for part in parts):
            # Đã đúng định dạng YYYY-MM-DD, giữ nguyên
            return date_value
        elif len(parts) == 2 and all(part.isdigit() for part in parts):
            # Chỉ có YYYY-MM, thêm -01
            return f"{date_value}-01"
        else:
            print(f"Warning: Invalid date format for {date_value}, keeping original")
            return date_value  # Giữ nguyên để debug
    # Nếu là datetime/Timestamp
    elif isinstance(date_value, (pd.Timestamp, datetime)):
        return date_value.strftime("%Y-%m-%d")
    else:
        print(f"Warning: Unsupported type for {date_value}: {type(date_value)}")
        return str(date_value)  # Chuyển thành chuỗi để tránh lỗi


def download_yah_prices_intraday_by_code(ticker="AAPL", pInterval="5m"):
    try:
        price_data = pd.DataFrame()
        price_data = download_yah_prices_by_api(
            pCodesource=ticker,
            pInterval=pInterval,
            pNbdays=60,
            Hour_adjust=0,
        )
        price_data = price_data[
            [
                "open",
                "high",
                "low",
                "close",
                "close_adj",
                "volume",
                "date",
                "datetime",
                "timestamp",
            ]
        ]
        price_data = price_data.rename(columns={"close_adj": "adjclose"})
        if price_data.empty:
            price_data = yf.download(
                ticker, period="max", auto_adjust=False, interval=pInterval
            )
            if price_data.empty:
                symbol = yf.Ticker(ticker)
                price_data = symbol.history(
                    period="max", auto_adjust=False, interval=pInterval
                )[["Open", "High", "Low", "Close", "Adj Close", "Volume"]]
                if price_data.empty:
                    print(f"⚠ Warning: No data for {ticker}")
                    return pd.DataFrame()

        if isinstance(price_data.columns, pd.MultiIndex):
            price_data.columns = price_data.columns.get_level_values(0)
        price_data = price_data.loc[:, ~price_data.columns.duplicated()]

        price_data = price_data.reset_index()
        price_data["ticker"] = ticker
        price_data = clean_colnames(price_data)

        if "date" in price_data.columns:
            price_data["date"] = pd.to_datetime(
                price_data["date"], utc=True
            ).dt.tz_localize(None)
        else:
            price_data["date"] = price_data["datetime"].dt.strftime("%Y-%m-%d")

        price_data["datetime"] = pd.to_datetime(
            price_data["datetime"], utc=True
        ).dt.tz_localize(None)

        price_data["timestamp"] = price_data["datetime"]

        price_data = update_updated(price_data)
        price_data = price_data.drop(columns=["index"], errors="ignore")
        view_data(price_data)
        return price_data
    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()


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
        # pNbdays = period_days.get(period)
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
            # price_data.columns = price_data.columns.droplevel(1)
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


def download_yah_info_by_code(ticker="AAPL"):
    # symbol = yf.Ticker('MSFT')
    symbol = yf.Ticker(ticker)
    info_data = symbol.info
    info_data = clean_colnames(info_data)

    return info_data


def download_yah_financial_by_code(ticker="AAPL"):
    symbol = yf.Ticker(ticker)

    balance_sheet = symbol.balance_sheet.reset_index()
    balance_sheet["dataset"] = "BALANCE_SHEET"

    cash_flow = symbol.cash_flow.reset_index()
    cash_flow["dataset"] = "CASH_FLOW"

    income_stat = symbol.income_stmt.reset_index()
    income_stat["dataset"] = "INCOME_STATEMENT"

    financial_data = pd.concat(
        [balance_sheet, cash_flow, income_stat], ignore_index=True
    )
    financial_data = clean_colnames(financial_data)

    return financial_data


def download_yah_shares_by_code(ticker="AAPL"):
    symbol = yf.Ticker(ticker)
    shares_data = symbol.get_shares_full()
    shares_data = pd.DataFrame(list(shares_data.items()), columns=["date", "sharesout"])
    shares_data = clean_colnames(shares_data)
    shares_data = shares_data.drop_duplicates(subset=["date"], keep="last")
    return shares_data


# FOR STOCK
def download_yah_shares_html_by_code(ticker="AAPL"):
    shares_data = pd.DataFrame()
    headers = {
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        # "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    pURL = f"https://finance.yahoo.com/quote/{ticker}/key-statistics/"

    response = requests.get(pURL, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    try:
        float_value = soup.select_one(
            "#nimbus-app > section > section > section > article > article > div > section:nth-child(2) > div > section:nth-child(2) > table > tbody > tr:nth-child(5) > td.value.yf-vaowmx"
        ).text.strip()
        float_value = convert_number(float_value)
    except:
        float_value = None

    try:
        sharesout_value = soup.select_one(
            "#nimbus-app > section > section > section > article > article > div > section:nth-child(2) > div > section:nth-child(2) > table > tbody > tr:nth-child(3) > td.value.yf-vaowmx"
        ).text.strip()
        sharesout_value = convert_number(sharesout_value)
    except:
        sharesout_value = None

    shares_data = pd.DataFrame({"sharesout": [sharesout_value], "float": [float_value]})

    return shares_data


def download_yah_marketcap_html_by_code(ticker="AAPL"):

    pURL = f"https://finance.yahoo.com/quote/{ticker}/"
    headers = {
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        # "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    response = requests.get(pURL, headers=headers)
    print(response)
    soup = BeautifulSoup(response.content, "html.parser")
    try:
        marketcap_value = soup.select_one(
            "#nimbus-app > section > section > section > article > div.container.yf-gn3zu3 > ul > li:nth-child(9) > span.value.yf-gn3zu3 > fin-streamer"
        ).text.strip()
        marketcap_value = convert_number(marketcap_value)
    except:
        marketcap_value = None

    return marketcap_value
