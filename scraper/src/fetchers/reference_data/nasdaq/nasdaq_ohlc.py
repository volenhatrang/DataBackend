import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))

from common_fn import *
import shutil


def download_nasdaq_prices_day():

    project_folder = os.path.abspath("crawler")
    download_dir = os.path.join(project_folder, "temp")
    os.makedirs(download_dir, exist_ok=True)
    chrome_options = Options()
    prefs = {"download.default_directory": download_dir}
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--force-device-scale-factor=0.9")

    driver = webdriver.Chrome(service=Service(), options=chrome_options)

    url = "https://www.nasdaq.com/market-activity/stocks/screener"
    driver.get(url)
    time.sleep(5)
    df = pd.DataFrame()
    try:
        download_button = driver.find_element(
            By.CSS_SELECTOR,
            "body > div.dialog-off-canvas-main-canvas > div > main > div.page__content > article > div > div.nsdq-bento-layout__main.nsdq-c-band.nsdq-c-band--white.nsdq-u-padding-top-md.nsdq-u-padding-bottom-md.nsdq-c-band__overflow_hidden > div.nsdq-l-layout-container.nsdq-l-layout-container--contained.nsdq-bento-ma.nsdq-u-padding-top-none.nsdq-u-padding-bottom-none.nsdq-u- > div > div.nsdq-sticky-container > div > div > div:nth-child(2) > div > div.jupiter22-c-symbol-screener__container > div.jupiter22-c-table-container > div.jupiter22-c-table__download > button",
        )
        download_button.click()
        print("✅ Download button clicked!")

        trading_date = driver.find_element(
            By.CSS_SELECTOR,
            "body > div.dialog-off-canvas-main-canvas > div > main > div.page__content > article > div > div.nsdq-bento-layout__main.nsdq-c-band.nsdq-c-band--white.nsdq-u-padding-top-md.nsdq-u-padding-bottom-md.nsdq-c-band__overflow_hidden > div.nsdq-l-layout-container.nsdq-l-layout-container--contained.nsdq-bento-ma.nsdq-u-padding-top-none.nsdq-u-padding-bottom-none.nsdq-u- > aside > nsdq-market-status-desktop > div > div.jupiter22-market-status.hide-loader > div.jupiter22-market-status--card.market-closed.loaded > span",
        ).text.strip()

        trading_date = datetime.strptime(trading_date, "%b %d, %Y")

        # print(trading_date.strftime("%Y-%m-%d"))  # 2025-03-13
        time.sleep(10)

        files = os.listdir(download_dir)
        files = [os.path.join(download_dir, f) for f in files if f.endswith(".csv")]
        latest_file = max(files, key=os.path.getctime) if files else None

        if latest_file:
            print(f"📥 Downloaded file: {latest_file}")

            df = pd.read_csv(latest_file)
            df["Close"] = df["Last Sale"].replace("[\$,]", "", regex=True).astype(float)
            df["Change"] = df["Net Change"].astype(float)
            df["Rt"] = df["% Change"].str.replace("%", "").astype(float)
            df["Market_cap"] = (
                pd.to_numeric(df["Market Cap"], errors="coerce").fillna(0).astype(int)
            )
            df["Volume"] = (
                pd.to_numeric(df["Volume"], errors="coerce").fillna(0).astype(int)
            )
            df = df.drop(columns=["Net Change", "% Change", "Last Sale", "Market Cap"])
            df = df.rename(columns={"IPO Year": "IPO_year"})
            df["Date"] = trading_date
            print(df.head())
        else:
            df = pd.DataFrame()
            print("⚠️ No CSV file was found in the download directory.")

    except Exception as e:
        print(f"❌ Error: {e}")

    driver.quit()

    try:
        shutil.rmtree(download_dir)
        print(f"🗑️ Deleted temp folder: {download_dir}")
    except Exception as e:
        print(f"⚠️ Error deleting folder: {e}")

    return df


def download_blg_prices_by_list(
    xList=[
        "USDVND:CUR",
        "USDJPY:CUR",
        "USDEUR:CUR",
        "USDGBP:CUR",
        "USDAUD:CUR",
        "USDCAD:CUR",
        "EURUSD:CUR",
        "USDRUB:CUR",
    ],
    p_frequency="1_DAY",
    calculate_day_change_var=True,
):

    import itertools

    xList = (
        list(itertools.chain.from_iterable(xList))
        if any(isinstance(i, list) for i in xList)
        else xList
    )

    xString = ",".join(map(str, xList))
    url = f"https://www.bloomberg.com/markets/api/comparison/data-timeseries?securities={xString}&securityType=COMMON_STOCK&timeFrame={p_frequency}&locale=en"
    print("URL:", url)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.5",
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("Error fetching data, status code:", response.status_code)
        return None

    x_web = response.text
    if len(x_web) <= 10:
        print("No valid data received.")
        return None

    try:
        xcur1 = json.loads(x_web)
    except json.JSONDecodeError:
        print("Error decoding JSON response.")
        return None

    if not xcur1:
        return None

    data1 = pd.DataFrame(xcur1.get("fieldDataCollection", []))
    if not data1.empty:
        data1.columns = data1.columns.str.lower().str.replace(" |-", "", regex=True)
        if "pricedate" in data1.columns:
            data1["date"] = pd.to_datetime(data1["pricedate"], format="%m/%d/%Y")
        data1["timestampvn"] = pd.to_datetime(data1["lastupdateiso"]) + pd.Timedelta(
            hours=7
        )
        data1["timestamp"] = pd.to_datetime(data1["lastupdateiso"])
        data1 = data1.assign(
            source="BLG",
            codesource=data1["id"],
            close=data1["price"],
            change=data1["pricechange1day"],
            var=data1["percentchange1day"] / 100,
        )

    time_series = xcur1.get("timeSeriesCollection", {}).get("price", [])
    data_list = []

    for k, series in enumerate(time_series):
        df = pd.DataFrame(series)
        if not df.empty:
            df.columns = df.columns.str.lower().str.replace(" |-", "", regex=True)
            df["codesource"] = xList[k]
            data_list.append(df)

    data_all = pd.concat(data_list, ignore_index=True) if data_list else pd.DataFrame()

    if "datetime" in data_all.columns:
        data_all["datetime"] = data_all["datetime"].astype(str)

    if p_frequency == "1_DAY":
        data_all["timestamp"] = pd.to_datetime(
            data_all["datetime"].str.replace("Z", "").str.replace("T", " ")
        )
        data_all["date"] = data_all["datetime"].str[:10].astype(str)
        data_all.rename(columns={"value": "close"}, inplace=True)
    else:
        data_all["date"] = pd.to_datetime(data_all["date"].astype(str))
        data_all.rename(columns={"value": "close"}, inplace=True)

    if calculate_day_change_var and p_frequency != "1_DAY":
        data_all.sort_values(by=["codesource", "date"], inplace=True)
        data_all["change"] = data_all.groupby("codesource")["close"].diff()
        data_all["var"] = data_all.groupby("codesource")["close"].pct_change()

    return data1, data_all


def download_cnbc_prices_by_code(time_range="1D", p_code="@VX.1"):
    initial_url = (
        f"https://webql-redesign.cnbcfm.com/graphql?operationName=getQuoteChartData"
        f"&variables=%7B%22symbol%22%3A%22{p_code}%22%2C%22timeRange%22%3A%22{time_range}%22%7D"
        f"&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A"
        f"%2261b6376df0a948ce77f977c69531a4a8ed6788c5ebcdd5edd29dd878ce879c8d%22%7D%7D"
    )

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    }

    response = requests.get(initial_url, headers=headers)
    if response.status_code != 200:
        print(f"Error fetching data for {p_code}: HTTP {response.status_code}")
        return pd.DataFrame()

    json_data = response.json()
    chart_data = json_data.get("data", {}).get("chartData", {})

    if not chart_data:
        print(f"{p_code} = NOT AVAILABLE")
        return pd.DataFrame()

    price_bars = chart_data.get("priceBars", [])
    all_symbols = chart_data.get("allSymbols", [])
    name = (
        all_symbols[0]["name"] if isinstance(all_symbols, list) and all_symbols else ""
    )
    short_name = (
        all_symbols[0]["shortName"]
        if isinstance(all_symbols, list) and all_symbols
        else ""
    )
    if not price_bars:
        return pd.DataFrame(
            {
                "codesource": [chart_data.get("symbol", "")],
                "name": [chart_data.get("allSymbols", {}).get("name", "")],
                "short_name": [chart_data.get("allSymbols", {}).get("shortName", "")],
                "source": ["CNBC"],
                "timeRange": [time_range],
            }
        )

    df = pd.DataFrame(price_bars)
    df["codesource"] = chart_data.get("symbol", "")
    df["name"] = name
    df["short_name"] = short_name
    df["source"] = "CNBC"
    df["timeRange"] = time_range
    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["high"] = pd.to_numeric(df["high"], errors="coerce")
    df["low"] = pd.to_numeric(df["low"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["tradeTime"] = pd.to_datetime(
        df["tradeTime"], format="%Y%m%d%H%M%S", errors="coerce"
    )

    df.drop(columns=["tradeTimeinMills"], errors="ignore", inplace=True)

    cols = ["codesource", "name", "short_name", "source", "timeRange"] + [
        col
        for col in df.columns
        if col not in ["codesource", "name", "short_name", "source", "timeRange"]
    ]
    df = df[cols]

    print(df.head())
    return df


def download_nasdaq_prices_by_code(pCodesource="IBIT", pNbdays=100000, typ="etf"):
    data = pd.DataFrame()
    # typ = 'etf'
    # typ = 'stocks'
    # typ = 'index'
    # typ = 'crypto'
    # typ = 'currencies'
    # typ = 'mutualfunds'

    purl = f"https://api.nasdaq.com/api/quote/{pCodesource}/historical?assetclass={typ}&fromdate=1900-01-01&limit={pNbdays}&todate={datetime.today().date()}"

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
        "Accept-Language": "en-US,en;q=0.5",
    }

    response = requests.get(purl, headers=headers)

    if response.status_code == 200:
        json_data = response.json()

        if "data" in json_data and "tradesTable" in json_data["data"]:
            data = pd.DataFrame(json_data["data"]["tradesTable"]["rows"])

        if not data.empty:
            data["date"] = pd.to_datetime(data["date"], format="%m/%d/%Y")

            data["volume"] = data["volume"].str.replace(",", "").astype(float)

            data["codesource"] = pCodesource
            data["source"] = "NASDAQ"

            print(data.head())

    return data


def download_nasdaq_prices_intraday_by_code(Symbol="IBIT", typ="etf"):
    # Define the URL for the Nasdaq API endpoint
    url = f"https://api.nasdaq.com/api/quote/{Symbol}/chart?assetClass={typ}&charttype=real"

    # Set up the headers, including a User-Agent to mimic a browser request
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": f"https://www.nasdaq.com/market-activity/{typ}/{Symbol.lower()}/real-time",
    }
    df = pd.DataFrame()
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()

        if "data" in data and "chart" in data["data"]:
            chart_data = data["data"]["chart"]
            df = pd.DataFrame(
                [
                    {
                        "timestamp": datetime.utcfromtimestamp(item["x"] / 1000),
                        "time": item["z"]["time"],
                        "shares_traded": int(item["z"]["shares"].replace(",", "")),
                        "close": float(item["z"]["price"].replace("$", "")),
                        "prev_close": float(item["z"]["prevCls"].replace("$", "")),
                    }
                    for item in chart_data
                ]
            )
            df["ticker"] = Symbol
            print(df.head())
        else:
            print("Chart data is not available in the response.")
    else:
        print(f"Failed to retrieve data: {response.status_code}")

    return df
