import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))

from common_fn import *


# ==============================================================================
# PRICES INTRADAY
# ==============================================================================
def download_caf_prices_intraday_by_code(pCode="VIC"):
    Final_Data = pd.DataFrame()
    last_trading = get_last_trading_day(max(9, datetime.now().hour), to_prompt=True)
    pURL = f"https://s.cafef.vn/ajax/khoplenh.aspx?order=time&dir=up&symbol={pCode}&date={last_trading}"
    print("=" * 40)
    print(pURL)

    try:
        response = requests.get(pURL)
        response.raise_for_status()
        content = response.text
    except Exception as e:
        print("Error fetching URL:", e)
        return pd.DataFrame()

    try:
        tables = pd.read_html(StringIO(content), flavor="lxml")
    except Exception as e:
        print("Error reading HTML tables:", e)
        tables = []

    if not tables:
        return pd.DataFrame()
    else:
        xData = tables[0]
        if xData.shape[1] == 5 and xData.shape[0] > 0:
            xData.columns = [
                str(col).strip().lower().replace(" ", "_") for col in xData.columns
            ]

            if len(xData.columns) == 5:
                xData.columns = ["x1", "x2", "x3", "x4", "x5"]

            last_val = xData["x2"].apply(
                lambda s: 1000 * clean_number(extract_word(s, 1))
            )
            change_val = xData["x2"].apply(
                lambda s: 1000 * clean_number(extract_word(s, 2))
            )
            varpc_val = xData["x2"].apply(lambda s: clean_varpc(extract_word(s, 3)))
            Final_Data = pd.DataFrame(
                {
                    "codesource": pCode,
                    "code": f"STKVN{pCode}",
                    "source": "CAF",
                    "date": last_trading,
                    "time": xData["x1"],
                    "volume": xData["x3"],
                    "volume_total": xData["x4"].apply(clean_number),
                    "last": last_val,
                    "change": change_val,
                    "varpc": varpc_val,
                }
            )
            Final_Data = update_updated(Final_Data)
            view_data(Final_Data)
        return Final_Data


def download_c68_prices_intraday_by_code(pCode="VND"):

    final_data = pd.DataFrame()
    pURL = "https://www.cophieu68.vn/quote/summary.php?id=" + pCode.lower()
    print("\n" + "=" * 40)
    print(pURL)

    try:
        response = requests.get(pURL)
        response.raise_for_status()
        content = response.text
    except Exception as e:
        print("Error fetching URL:", e)
        return final_data

    try:
        soup = BeautifulSoup(content, "lxml")
        for tag in soup.find_all(attrs={"colspan": True}):
            colspan_value = tag.get("colspan", "1")
            try:
                int(colspan_value)
            except ValueError:
                tag["colspan"] = "1"

        tables = pd.read_html(StringIO(str(soup)), flavor="lxml")
    except Exception as e:
        print("Error reading HTML tables:", e)
        tables = []

    if len(tables) >= 4:
        last_trading = get_last_trading_day(max(9, datetime.now().hour), to_prompt=True)
        xData = tables[3]

        def process_numeric(col):
            return 1000 * pd.to_numeric(
                col.astype(str).str.replace(",", ""), errors="coerce"
            )

        final_data = pd.DataFrame(
            {
                "source": "C68",
                "codesource": pCode,
                "code": "STKVN" + pCode,
                "date": last_trading,
                "timestamp": xData.iloc[:, 0].astype(str) + ":00",
                "last": process_numeric(xData.iloc[:, 1]),
                "change": process_numeric(xData.iloc[:, 2]),
                "last_volume": process_numeric(xData.iloc[:, 3]),
                "cumul_volume": process_numeric(xData.iloc[:, 4]),
            }
        )
        final_data = final_data[final_data["last"].notna()]
        final_data = update_updated(final_data)

        view_data(final_data)

    return final_data


def download_ent_prices_intraday_by_code(
    type="index",
    codesource="VN30",
    code="INDVN30",
    NbMinutes=11 * 365 * 24 * 60,
    OffsetDays=0,
    pInterval="1",
):
    # Calculate End.time and Start.time in epoch time
    end_time = round(datetime.now().timestamp()) - (OffsetDays * 24 * 60 * 60 - 1)
    start_time = round(end_time - NbMinutes * 60)

    # Construct the URL
    p_url = f"https://services.entrade.com.vn/chart-api/v2/ohlcs/{type}?from={start_time}&to={end_time}&symbol={codesource}&resolution={pInterval}"
    print("Download URL:", p_url)

    # Request data from URL
    response = requests.get(p_url)

    # Print raw JSON data for inspection
    try:
        data = response.json()
        print(
            "Raw JSON data:", data
        )  # Inspect the structure here to ensure it's what we expect
    except ValueError as e:
        print(f"Failed to parse JSON data. Error: {e}")
        return pd.DataFrame()

    # Convert data to DataFrame if available
    if len(data) > 1:
        df = pd.DataFrame(data)
        df["timestamp_vn"] = pd.to_datetime(df["t"], unit="s") + timedelta(hours=7)
        df["open"] = df["o"] * 1000
        df["high"] = df["h"] * 1000
        df["low"] = df["l"] * 1000
        df["close"] = df["c"] * 1000
        df["volume"] = df["v"]

        # Additional calculations
        df["hhmmss"] = df["timestamp_vn"].dt.strftime("%H:%M")
        df["date"] = df["timestamp_vn"].dt.date
        df["source"] = "ENT"
        df["code"] = code
        df["codesource"] = codesource
        df["ticker"] = codesource
        df["type"] = "IND" if type == "index" else "STK"
        df["timestamp"] = df["timestamp_vn"].astype(str)

        # Placeholder values for ref1 mappings
        ref1 = {"iso2": "VN", "iso3": "VNM", "country": "VIETNAM", "continent": "ASIA"}

        for col, val in ref1.items():
            df[col] = val
        df = df.drop(columns=["o", "h", "l", "c", "v", "t", "nextTime"])
        df = update_updated(df)

        view_data(df)
        return df
    else:
        print("No data rows available")
        return pd.DataFrame()


def download_vnd_prices_intraday_by_code(
    pCode="VNM", NbDaysBack=10000, minute=1, pSource="VND"
):
    start_time = (datetime.now() - timedelta(days=NbDaysBack)).replace(
        hour=9, minute=0, second=0
    )
    posix_start = int(start_time.timestamp())

    end_time = datetime.now()
    posix_end = int(end_time.timestamp())

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    }
    pURL = (
        f"https://dchart-api.vndirect.com.vn/dchart/history?resolution={minute}"
        f"&symbol={pCode}&from={posix_start}&to={posix_end}"
    )

    response = requests.get(pURL, headers=headers)
    data = response.json()

    xData = pd.DataFrame(data)

    if "t" in xData.columns:
        xData["timestamp"] = pd.to_datetime(xData["t"], unit="s").astype(str)

    if "s" in xData.columns and "ok" in xData["s"].values:
        Final_Data = xData[xData["s"] == "ok"].copy()
        Final_Data = Final_Data.assign(
            source=pSource,
            ticker=pCode,
            codesource=pCode,
            code="STKVN" + pCode,
            date=pd.to_datetime(Final_Data["timestamp"]).dt.date,
            open=Final_Data["o"] * 1000,
            high=Final_Data["h"] * 1000,
            low=Final_Data["l"] * 1000,
            last=Final_Data["c"] * 1000,
            volume=Final_Data["v"],
        )
        Final_Data = Final_Data.drop(
            columns=["o", "h", "l", "c", "v", "t", "s"], errors="ignore"
        )
        Final_Data = update_updated(Final_Data)

        view_data(Final_Data)
        return Final_Data

    return pd.DataFrame()


# ==============================================================================
# PRICES DAY HISTORY
# ==============================================================================
def download_caf_prices_by_code(
    p_codesource="vnindex", code_int="INDVNINDEX", ToHistory=False
):
    data_list = []
    to_continue = True
    k = 1

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    }

    while to_continue:
        # Construct the URL
        url = f"https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol={p_codesource}&StartDate=&EndDate=&PageIndex={k}&PageSize=100"

        try:
            # Make the request and parse JSON
            response = requests.get(url, headers=headers)
            x = response.json()

            # Check if data is available
            if "Data" in x and "Data" in x["Data"] and len(x["Data"]["Data"]) > 0:
                # Convert data to DataFrame
                df = pd.DataFrame(x["Data"]["Data"])

                # Clean and rename columns
                df = df.rename(columns=lambda col: col.strip().lower())
                df["thaydoi"] = df["thaydoi"].astype(str)
                df["khoiluongkhoplenh"] = df["khoiluongkhoplenh"].astype(str)
                df["giatrikhoplenh"] = df["giatrikhoplenh"].astype(str)
                # Select and transform columns as per requirements
                df = pd.DataFrame(
                    {
                        "source": "CAF",
                        "codesource": p_codesource.upper(),
                        "ticker": p_codesource.upper(),
                        "date": pd.to_datetime(
                            df["ngay"], format="%d/%m/%Y", errors="coerce"
                        ),
                        "open": pd.to_numeric(df["giamocua"], errors="coerce"),
                        "high": pd.to_numeric(df["giacaonhat"], errors="coerce"),
                        "low": pd.to_numeric(df["giathapnhat"], errors="coerce"),
                        "close_adj": pd.to_numeric(df["giadieuchinh"], errors="coerce"),
                        "close": pd.to_numeric(df["giadongcua"], errors="coerce"),
                        "change": pd.to_numeric(
                            df["thaydoi"].str.split("(", expand=True)[0],
                            errors="coerce",
                        ),
                        "varpc": pd.to_numeric(
                            df["thaydoi"].str.extract(r"\((.*?)%\)")[0], errors="coerce"
                        ),
                        "volume": pd.to_numeric(
                            df["khoiluongkhoplenh"].str.replace(",", ""),
                            errors="coerce",
                        ),
                        "turnover": pd.to_numeric(
                            df["giatrikhoplenh"].str.replace(",", ""), errors="coerce"
                        ),
                    }
                )

                data_list.append(df)
                k += 1  # Move to the next page
                if not ToHistory:
                    break
            else:
                to_continue = False  # Stop if there's no more data

        except Exception as e:
            print(f"Error retrieving or processing data: {e}")
            to_continue = False  # Stop in case of error

    # Combine all pages into a single DataFrame
    if data_list:
        data_all = pd.concat(data_list, ignore_index=True)
        data_all = data_all.sort_values(by="date").reset_index(drop=True)

        # Calculate additional columns
        data_all["reference"] = data_all["close"] - data_all["change"]
        data_all["rt"] = data_all["change"] / data_all["reference"]
        if code_int:
            data_all["code"] = code_int.upper()
        else:
            data_all["code"] = f"STKVN{p_codesource}"
        data_all = update_updated(data_all)
        view_data(data_all)
        return data_all
    else:
        return pd.DataFrame()  # Return empty DataFrame if no data


def download_vnd_prices_by_code(
    type="index", p_codesource="VNINDEX", code_int="INDVNINDEX"
):
    data_list = []
    to_continue = True
    k = 1

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    }

    while to_continue:
        # Construct the URL
        if type == "index":
            url = f"https://api-finfo.vndirect.com.vn/v4/vnmarket_prices?sort=date&q=code:{p_codesource}~date:gte:2000-07-29~date:lte:{date.today()}&size=100&page={k}"
        else:
            url = f"https://api-finfo.vndirect.com.vn/v4/stock_prices?sort=date&q=code:{p_codesource}&size=100&page={k}"
        try:
            # Make the request and parse JSON
            response = requests.get(url, headers=headers)
            x = response.json()

            # Check if data is available
            if "data" in x and len(x["data"]) > 0:
                # Convert data to DataFrame
                df = pd.DataFrame(x["data"])
                df.columns
                # Clean and rename columns
                df = df.rename(columns=lambda col: col.strip().lower())

                # Select and transform columns as per requirements
                df = pd.DataFrame(
                    {
                        "source": "VND",
                        "codesource": p_codesource.upper(),
                        "ticker": p_codesource.upper(),
                        "date": pd.to_datetime(df["date"]).dt.date,
                        "open": pd.to_numeric(df["open"], errors="coerce"),
                        "high": pd.to_numeric(df["high"], errors="coerce"),
                        "low": pd.to_numeric(df["low"], errors="coerce"),
                        "close": pd.to_numeric(df["close"], errors="coerce"),
                        "change": pd.to_numeric(df["change"], errors="coerce"),
                        "rt": pd.to_numeric(df["pctchange"], errors="coerce"),
                        "volume": pd.to_numeric(df["nmvolume"], errors="coerce"),
                        "turnover": pd.to_numeric(df["nmvalue"], errors="coerce"),
                    }
                )

                data_list.append(df)
                k += 1  # Move to the next page
            else:
                to_continue = False  # Stop if there's no more data

        except Exception as e:
            print(f"Error retrieving or processing data: {e}")
            to_continue = False  # Stop in case of error

    # Combine all pages into a single DataFrame
    if data_list:
        data_all = pd.concat(data_list, ignore_index=True)
        data_all = data_all.sort_values(by="date").reset_index(drop=True)

        # Calculate additional columns
        data_all["reference"] = data_all["close"] - data_all["change"]
        data_all["rt"] = data_all["change"] / data_all["reference"]
        data_all["code"] = code_int.upper()
        data_all = update_updated(data_all)

        view_data(data_all)
        return data_all
    else:
        return pd.DataFrame()  # Return empty DataFrame if no data


def download_ent_prices_by_code(
    type="index",
    codesource="VN30",
    code="INDVN30",
    NbMinutes=11 * 365 * 24 * 60,
    OffsetDays=0,
    pInterval="1",
):
    final_data = pd.DataFrame()
    final_data = download_ent_prices_by_code(
        type=type,
        codesource=codesource,
        code=code,
        NbMinutes=NbMinutes,
        OffsetDays=OffsetDays,
        pInterval=pInterval,
    )
    if final_data:
        final_data = final_data.sort_values(by=["code", "date", "timestamp_vn"])
        final_data = final_data.drop_duplicates(subset=["code", "date"], keep="last")

    final_data = update_updated(final_data)

    return final_data


def download_stb_prices_by_code(pCode="VND"):
    pURL = f"http://en.stockbiz.vn/Stocks/{pCode}/HistoricalQuotes.aspx"
    print("=" * 40)
    print(pURL)

    try:
        response = requests.get(pURL)
        response.raise_for_status()
        content = response.text
    except Exception as e:
        print("Error fetching URL:", e)
        return pd.DataFrame()

    try:
        soup = BeautifulSoup(content, "lxml")
        tables = pd.read_html(StringIO(str(soup)), flavor="lxml")
    except Exception as e:
        print("Error reading HTML tables:", e)
        return pd.DataFrame()

    if len(tables) < 23:
        print("Không tìm thấy đủ bảng (ít nhất 23 bảng).")
        return pd.DataFrame()

    xTable = tables[22]
    xData = xTable.iloc[:, :9].copy()

    xHeader = [str(val).strip().lower().replace(" ", "") for val in xData.iloc[0]]
    xData.columns = xHeader

    xData = xData.iloc[1:-1].copy()

    def to_numeric(s):
        try:
            return float(str(s).replace(",", ""))
        except Exception:
            return None

    def extract_first_token(s, sep="/"):
        try:
            return str(s).split(sep)[0]
        except Exception:
            return s

    try:
        Final_Data = pd.DataFrame(
            {
                "codesource": pCode,
                "source": "STB",
                "code": f"STKVN{pCode}",
                "date": pd.to_datetime(
                    xData["date"], format="%m/%d/%Y", errors="coerce"
                ),
                "open": 1000 * xData["open"].apply(to_numeric),
                "high": 1000 * xData["high"].apply(to_numeric),
                "low": 1000 * xData["low"].apply(to_numeric),
                "close": 1000 * xData["close"].apply(to_numeric),
                "average": 1000 * xData["average"].apply(to_numeric),
                "close_adj": 1000 * xData["adjustedclose"].apply(to_numeric),
                "change": 1000
                * xData["change"].apply(
                    lambda s: to_numeric(extract_first_token(s, sep="/"))
                ),
                "volume": xData["volume"].apply(to_numeric),
            }
        )
    except Exception as e:
        print("Error processing data:", e)
        return pd.DataFrame()

    Final_Data.sort_values(by="date", inplace=True)
    Final_Data = update_updated(Final_Data)

    view_data(Final_Data)
    return Final_Data


def download_fant_prices_by_code(pCodesource="VIC", STOCK=True, CodeInt=""):
    """
    Download historical prices from FANT for a given symbol.

    Parameters:
        pCodesource (str): The source code (e.g., 'VIC').
        Starttime (int): Start time (unused in this example).
        STOCK (bool): If True, construct code as 'STKVN' + pCodesource; otherwise use CodeInt.
        CodeInt (str): Alternative code if STOCK is False.
        merge_ins_ref (bool): If True, merge additional reference data.

    Returns:
        pd.DataFrame: A DataFrame with historical price data and calculated fields.
    """
    print("-" * 40)
    print(f"TRAINEE_DOWNLOAD_FANT_PRICES_BY_CODE - {pCodesource}")
    start_time = datetime.now()

    end_date = date.today()
    start_date = date(1962, 1, 1)
    limit = (end_date - start_date).days

    Final_Data = pd.DataFrame()

    # Build the URL
    pURL = (
        f"https://restv2.fireant.vn/symbols/{pCodesource}/historical-quotes?"
        f"startDate={start_date}&endDate={end_date}&offset=0&limit={limit}"
    )

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg",
        "Sec-Ch-Ua": '"Microsoft Edge";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
    }

    try:
        response = requests.get(pURL, headers=headers)
        response.raise_for_status()
        # Get JSON response data
        json_data = response.json()
    except Exception as e:
        print("Error fetching data:", e)
        return Final_Data

    # Proceed only if the JSON data has more than 5 items
    if len(json_data) > 5:
        try:
            data_table = pd.DataFrame(json_data)
        except Exception as e:
            print("Error converting JSON to DataFrame:", e)
            return Final_Data

        if not data_table.empty:
            data_table["source"] = "FANT"
            data_table["codesource"] = pCodesource
            if STOCK:
                data_table["code"] = "STKVN" + pCodesource
            else:
                data_table["code"] = CodeInt if CodeInt else None

            if "date" in data_table.columns:
                data_table["date"] = pd.to_datetime(data_table["date"], errors="coerce")

            data_table["updated"] = sys_time()

            data_table.columns = [
                str(col).strip().lower().replace(" ", "_") for col in data_table.columns
            ]
            data_table["open"] = data_table["unit"] * data_table["priceopen"]
            data_table["close_adj"] = data_table["unit"] * data_table["priceclose"]
            data_table["high"] = data_table["unit"] * data_table["pricehigh"]
            data_table["low"] = data_table["unit"] * data_table["pricelow"]
            data_table["average"] = data_table["unit"] * data_table["priceaverage"]
            data_table["close"] = data_table["unit"] * data_table["priceclose"]
            data_table["reference"] = data_table["unit"] * data_table["pricebasic"]
            data_table["close_unadj"] = data_table["unit"] * data_table["priceclose"]
            data_table["volume"] = data_table["totalvolume"]
            data_table["turnover"] = data_table["totalvalue"]

            data_table.loc[data_table["reference"] == 0, "reference"] = pd.NA
            data_table.loc[data_table["reference"].notna(), "change"] = (
                data_table["close"] - data_table["reference"]
            )
            data_table.loc[data_table["reference"].notna(), "varpc"] = (
                100 * data_table["change"] / data_table["reference"]
            )
            data_table.loc[data_table["reference"].notna(), "rt"] = (
                data_table["varpc"] / 100
            )

            Final_Data = data_table[
                [
                    "source",
                    "symbol",
                    "codesource",
                    "code",
                    "date",
                    "open",
                    "close_adj",
                    "high",
                    "low",
                    "average",
                    "close",
                    "reference",
                    "close_unadj",
                    "volume",
                    "turnover",
                    "change",
                    "varpc",
                    "rt",
                ]
            ].copy()
            Final_Data.rename(columns={"symbol": "ticker"}, inplace=True)
            Final_Data = update_updated(Final_Data)
            Final_Data.sort_values(by="date", inplace=True)
            view_data(Final_Data)
    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()
    print(
        f"TRAINEE_DOWNLOAD_FANT_PRICES_BY_CODE - Duration = {format_number(duration, 2)} secs"
    )
    print("-" * 120)
    return Final_Data


# ==============================================================================
# SHARES
# ==============================================================================


def download_vnd_shares_by_code(pCode="VNM"):
    Final_data = pd.DataFrame()

    current_hour = int(datetime.now().strftime("%H"))
    LastTrading = get_last_trading_day(current_hour, to_prompt=True)
    pURL = (
        "https://api-finfo.vndirect.com.vn/v4/ratios/latest?filter=ratioCode:MARKETCAP,"
        "NMVOLUME_AVG_CR_10D,PRICE_HIGHEST_CR_52W,PRICE_LOWEST_CR_52W,OUTSTANDING_SHARES,"
        "FREEFLOAT,BETA,PRICE_TO_EARNINGS,PRICE_TO_BOOK,DIVIDEND_YIELD,BVPS_CR,"
        "&where=code:"
        + pCode
        + "~reportDate:gt:"
        + str(LastTrading)
        + "&order=reportDate&fields=ratioCode,value"
    )
    print(pURL)

    try:
        response = requests.get(pURL)
        response.raise_for_status()
        x = response.json()
    except Exception as e:
        print("Error fetching URL:", e)
        return Final_data

    xData = x.get("data", [])

    if len(xData) > 0 and any(
        item.get("ratioCode") == "OUTSTANDING_SHARES" for item in xData
    ):
        shares = [
            item for item in xData if item.get("ratioCode") == "OUTSTANDING_SHARES"
        ]
        if shares:
            shares_value = shares[0].get("value")

            Final_data = pd.DataFrame(
                {
                    "source": ["VND"],
                    "codesource": [pCode],
                    "ticker": [pCode],
                    "date": [LastTrading],
                    "code": [f"STKVN{pCode}"],
                    "sharesout": [shares_value],
                    "shareslis": [shares_value],
                }
            )
    Final_data = update_updated(Final_data)
    view_data(Final_data)

    return Final_data


def download_caf_shares_by_code(
    pCode="VNM",
    URL="",
    ToAdd=False,
    data_path="/home/baongoc2001/project/Bigdata-System/airflow/data/list_link_cafef.txt",
):
    Final_Data = pd.DataFrame()
    CAF_LINK = None

    if not URL:
        try:
            CAF_LINKS = pd.read_csv(data_path, sep=",", dtype=str)
            if "code" not in CAF_LINKS.columns or "URL" not in CAF_LINKS.columns:
                raise KeyError(
                    "Columns 'code' and 'URL' not found in the file. Please check the data structure."
                )

            CAF_LINKS = CAF_LINKS.drop_duplicates(subset=["code", "URL"], keep="last")

            CAF_LINK = f"https://s.cafef.vn/{CAF_LINKS.loc[CAF_LINKS['ticker'] == pCode, 'link'].values[0]}"

        except Exception as e:
            print(f"Error reading CAF links: {e}")
            return Final_Data
    else:
        CAF_LINK = URL

    print(f"Processing URL: {CAF_LINK}")

    if CAF_LINK:
        response = requests.get(CAF_LINK)
        content = response.text

        soup = BeautifulSoup(content, "html.parser")

        shares_lis_text = soup.find("div", text="KLCP đang niêm yết:")
        if shares_lis_text:
            shares_lis = pd.to_numeric(
                shares_lis_text.find_next("div", class_="r")
                .text.replace(",", "")
                .strip(),
                errors="coerce",
            )
        else:
            shares_lis = None

        shares_out_text = soup.find("div", text="KLCP đang lưu hành:")
        if shares_out_text:
            shares_out = pd.to_numeric(
                shares_out_text.find_next("div", class_="r")
                .text.replace(",", "")
                .strip(),
                errors="coerce",
            )
        else:
            shares_out = None

        last_trading = get_last_trading_day(9, to_prompt=True)

        Final_Data = pd.DataFrame(
            {
                "ticker": [pCode],
                "codesource": [pCode],
                "source": ["CAF"],
                "code": [f"STKVN{pCode}"],
                "date": [last_trading],
                "sharesout": [shares_out],
                "shareslis": [shares_lis],
            }
        )

        if ToAdd and not Final_Data.empty:
            CAF_LINKS = pd.read_csv(data_path, sep=",", dtype=str)
            if f"STKVN{pCode}" not in CAF_LINKS["code"].values:
                new_entry = pd.DataFrame(
                    {
                        "ticker": [pCode],
                        "code": [f"STKVN{pCode}"],
                        "link": [CAF_LINK.replace("https://s.cafef.vn/", "")],
                        "URL": [CAF_LINK],
                        "updated": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                    }
                )
                CAF_LINKS = pd.concat(
                    [CAF_LINKS, new_entry], ignore_index=True
                ).drop_duplicates(subset=["code", "URL"], keep="last")
                CAF_LINKS.to_csv(data_path, sep="\t", index=False)
    Final_Data = update_updated(Final_Data)
    view_data(Final_Data)

    return Final_Data


# ==============================================================================
# PRICESBOARD
# ==============================================================================
# chua check cot
def download_tvsi_priceboard():
    Data_All = pd.DataFrame()
    pURL = "https://prs.tvsi.com.vn/get-detail-stock-by"
    current_hour_str = int(datetime.now().strftime("%H"))
    LastTrading = get_last_trading_day(current_hour_str, to_prompt=True)
    for Market in ["HSX", "HNX", "UPCOM"]:
        my_param = {
            "type": Market,
            "typeTab": "M",
            "sort": "SortName-up",
            "stockStr": "",
        }

        try:
            res = requests.post(pURL, data=my_param)
            res.raise_for_status()
        except Exception as e:
            print("Error during POST:", e)
            return pd.DataFrame()

        try:
            x_out = res.text
            x_data = json.loads(x_out)
        except Exception as e:
            print("Error parsing JSON:", e)
            return pd.DataFrame()
        try:
            dt = pd.DataFrame(
                [record.split("|") for record in x_data["arrDetailStock"]]
            )
        except Exception as e:
            print("Error writing/reading temporary file:", e)
            return pd.DataFrame()
        try:
            Final_Data = pd.DataFrame(
                {
                    "source": "TVSI",
                    "ticker": dt[0].astype(str).str.replace('"', ""),
                    "market": "UPC" if Market.upper() == "UPCOM" else Market,
                    "reference": dt[6].apply(
                        lambda s: extract_word(s, index=2, delimiter="*")
                    ),
                    "change": dt[15].apply(
                        lambda s: extract_word(s, index=2, delimiter="*")
                    ),
                    "last": dt[17].apply(
                        lambda s: extract_word(s, index=2, delimiter="*")
                    ),
                    "volume": dt[19].apply(
                        lambda s: extract_word(s, index=2, delimiter="*")
                    ),
                }
            )
        except Exception as e:
            print("Error creating Final_Data DataFrame:", e)
            return pd.DataFrame()
        Final_Data["code"] = "STKVN" + Final_Data["ticker"]
        Final_Data["reference"] = Final_Data["reference"].apply(
            lambda x: clean_numeric(x, 1000)
        )
        Final_Data["last"] = Final_Data["last"].apply(lambda x: clean_numeric(x, 1000))
        Final_Data["change"] = Final_Data["change"].apply(
            lambda x: clean_numeric(x, 1000)
        )
        Final_Data["volume"] = Final_Data["volume"].apply(
            lambda x: clean_numeric(x, 10)
        )
        Final_Data["close"] = pd.NA
        Final_Data["date"] = LastTrading
        now = datetime.now()
        hour = int(now.strftime("%H"))
        minute = int(now.strftime("%M"))
        is_after_1545 = (hour > 15) or (hour == 15 and minute >= 45)
        if is_after_1545 or (date.today() > LastTrading):
            Final_Data["close"] = Final_Data["last"]
            mask = Final_Data["volume"].isna() & (
                Final_Data["close"] == Final_Data["reference"]
            )
            Final_Data.loc[mask, "volume"] = 0
        Final_Data = Final_Data[Final_Data["ticker"].str.len() == 3]
        Data_All = pd.concat([Data_All, Final_Data], ignore_index=True)
    Data_All = update_updated(Data_All)
    view_data(Data_All)
    return Data_All


# sai cot
def download_vst_priceboard():
    Data_All = pd.DataFrame()
    pURL = "https://banggia.vietstock.vn/bang-gia/"

    options = webdriver.ChromeOptions()
    s = Service()
    options.add_argument("--headless")
    options.add_argument("--window-position=-2400,-2400")
    driver = webdriver.Chrome(service=s, options=options)

    current_hour_str = int(datetime.now().strftime("%H"))
    LastTrading = get_last_trading_day(current_hour_str, to_prompt=True)
    for Market in ["hose", "hnx", "upcom"]:
        driver.get(f"{pURL}{Market}")
        content = driver.find_element(By.XPATH, "//*").get_attribute("outerHTML")

        try:
            soup = BeautifulSoup(content, "lxml")
            tables = pd.read_html(StringIO(str(soup)), flavor="lxml")
        except Exception as e:
            print("Error reading HTML tables:", e)
            return pd.DataFrame()

        if not tables:
            return pd.DataFrame()
        else:
            xData = tables[1]

        xData["ticker"] = xData[0].astype(str).str.replace(r"\*", "", regex=True)
        Final_Data = pd.DataFrame(
            {
                "source": "VST",
                "market": (
                    "HSX" if Market == "hose" else "HNX" if Market == "hnx" else "UPC"
                ),
                "ticker": xData["ticker"],
                "date": LastTrading,
                "reference": xData[1].astype(str).str.replace(",", "", regex=False),
                "last": xData[6].astype(str).str.replace(",", "", regex=False),
                "change": xData[12],
                "volume": xData[21]
                .astype(str)
                .str.split("\n")
                .str[0]
                .str.replace(",", "", regex=False),
            }
        )
        Final_Data["reference"] = (
            pd.to_numeric(Final_Data["reference"], errors="coerce") * 1000
        )
        Final_Data["last"] = pd.to_numeric(Final_Data["last"], errors="coerce") * 1000
        Final_Data["change"] = (
            pd.to_numeric(Final_Data["change"], errors="coerce") * 1000
        )
        Final_Data["volume"] = pd.to_numeric(Final_Data["volume"], errors="coerce")
        Final_Data["code"] = "STKVN" + Final_Data["ticker"]
        Final_Data = Final_Data[Final_Data["ticker"].str.len() == 3]
        Final_Data = Final_Data.sort_values(by="code")

        Data_All = pd.concat([Data_All, Final_Data], ignore_index=True)
    driver.quit()

    Data_All = update_updated(Data_All)
    view_data(Data_All)
    return Data_All


# ==============================================================================
# FREE FLOAT
# ==============================================================================
def download_vnd_freefloat_by_code(pCode="VNM"):
    Final_data = pd.DataFrame()

    current_hour = int(datetime.now().strftime("%H"))
    LastTrading = get_last_trading_day(current_hour, to_prompt=True)
    pURL = (
        "https://api-finfo.vndirect.com.vn/v4/ratios/latest?filter=ratioCode:MARKETCAP,"
        "NMVOLUME_AVG_CR_10D,PRICE_HIGHEST_CR_52W,PRICE_LOWEST_CR_52W,OUTSTANDING_SHARES,"
        "FREEFLOAT,BETA,PRICE_TO_EARNINGS,PRICE_TO_BOOK,DIVIDEND_YIELD,BVPS_CR,"
        "&where=code:"
        + pCode
        + "~reportDate:gt:"
        + str(LastTrading)
        + "&order=reportDate&fields=ratioCode,value"
    )
    print(pURL)

    try:
        response = requests.get(pURL)
        response.raise_for_status()
        x = response.json()
    except Exception as e:
        print("Error fetching URL:", e)
        return Final_data

    xData = x.get("data", [])

    # Kiểm tra nếu xData có dữ liệu và chứa ratioCode "OUTSTANDING_SHARES"
    if len(xData) > 0 and any(item.get("ratioCode") == "FREEFLOAT" for item in xData):
        free_float = [item for item in xData if item.get("ratioCode") == "FREEFLOAT"]
        if free_float:
            free_float_value = free_float[0].get("value")

            Final_data = pd.DataFrame(
                {
                    "source": ["VND"],
                    "codesource": [pCode],
                    "ticker": [pCode],
                    "date": [LastTrading],
                    "code": [f"STKVN{pCode}"],
                    "free_float": [free_float_value],
                }
            )
    Final_data = update_updated(Final_data)
    view_data(Final_data)

    return Final_data


# chua check
def download_24hmoney_freefloat_by_code(pCode="AAA"):
    url = f"https://24hmoney.vn/stock/{pCode}"
    print(url)
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
        "Accept-Language": "en-US,en;q=0.5",
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve data for {pCode}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    xpath_list = [
        '//*[@id="__layout"]/div/div[2]/div[1]/div[6]/div[1]/div/table/tbody/tr[10]/td[1]/div/span[2]',
        '//*[@id="__layout"]/div/div/content/div/main/div[1]/div[7]/table/tbody/tr[10]/td[1]/div/span[2]',
    ]

    free_float = None

    for xpath in xpath_list:
        element = soup.select_one(
            xpath.replace('//*[@id="__layout"]', "")
        )  # Adjusted for BeautifulSoup
        if element:
            free_float = element.get_text(strip=True)
            break

    if free_float:
        free_float = re.sub(r"\D", "", free_float)  # Remove non-numeric characters
        free_float = int(free_float) if free_float else None
        print(f"DOWNLOAD_24HMONEY_FREEFLOATPC : {pCode} = {free_float}")
        return free_float
    else:
        print(f"Free float data not found for {pCode}")
        return None
