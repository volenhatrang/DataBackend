from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import pandas as pd
import os


def download_all_indices_tradingview(quote="us", tradingview_path = (
        "/opt/airflow/FINANCEDATASCRAPER/database/scraping_raw_json/tradingview"
    )):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-position=-2400,-2400")
    chrome_options.add_argument("start-maximized")
    chrome_options.add_argument("--no-proxy-server")
    chrome_options.add_argument("--force-device-scale-factor=0.9")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=2560,1440")

    # chrome_options.add_argument(
    #     "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    # )
    if quote:
        quote_list = [quote]
    else:
        quote_list = [
            "major",
            "snp",
            "us",
            "americas",
            "currency",
            "europe",
            "asia",
            "pacific",
            "middle-east",
            "africa",
        ]
    quote_mappings = {
        "major": "World Indices",
        "snp": "S&P 500",
        "us": "US Indices",
        "americas": "Americas Indices",
        "currency": "Currencies",
        "europe": "European Indices",
        "asia": "Asian Indices",
        "pacific": "Pacific Indices",
        "middle-east": "Middle East Indices",
        "africa": "African Indices",
    }
    all_data = []


    os.makedirs(tradingview_path, exist_ok=True)
    for quote in quote_list:
        s = Service()
        driver = webdriver.Chrome(service=s, options=chrome_options)
        driver.set_window_size(1920, 1080)
        try:
            driver.get(f"https://www.tradingview.com/markets/indices/quotes-{quote}/")
            table_rows = driver.find_elements(
                By.CSS_SELECTOR, "table.table-Ngq2xrcG tbody tr"
            )
            headers = driver.find_element(By.CLASS_NAME, "tableHead-RHkwFEqU")
            headers = headers.find_elements(By.TAG_NAME, "th")
            column_names = []
            for header in headers:
                header_text = header.text.strip()
                if not header_text:
                    header_text = header.get_attribute("data-field")
                if header_text:
                    column_names.append(header_text)

            rows = driver.find_elements(By.CLASS_NAME, "row-RdUXZpkv")
            row_keys = []
            cell_data_lists = []

            for row in rows:
                row_key = row.get_attribute("data-rowkey")

                if row_key:
                    row_keys.append(row_key)

                cells = row.find_elements(By.TAG_NAME, "td")
                cell_data = []
                for idx, cell in enumerate(cells):
                    if idx == 0:
                        symbol_text = ""
                        description_text = ""
                        try:
                            ticker_name_element = cell.find_element(
                                By.CSS_SELECTOR, "a.tickerName-GrtoTeat"
                            )
                            symbol_text = ticker_name_element.text.strip()
                        except:
                            pass
                        try:
                            description_element = cell.find_element(
                                By.CSS_SELECTOR, "sup.tickerDescription-GrtoTeat"
                            )
                            description_text = description_element.text.strip()
                        except:
                            pass
                        full_text = "\n".join(
                            filter(None, [symbol_text, description_text])
                        ).strip()
                    else:
                        full_text = cell.text.strip()
                    cell_data.append(full_text)
                if any(cell_data):
                    cell_data_lists.append(cell_data)

            df = pd.DataFrame(
                cell_data_lists, index=row_keys, columns=column_names
            ).reset_index()

            df.rename(columns={"index": "Exchange"}, inplace=True)
            split_df = df["Symbol"].str.split("\n", expand=True)

            if split_df.shape[1] < 2:
                split_df = split_df.reindex(columns=range(2), fill_value="")

            df[["Ticker", "Name"]] = split_df[[0, 1]].fillna("")
            df = df.drop(columns=["Symbol"])

            currency_columns = ["Price", "Change", "High", "Low"]
            for column in currency_columns:
                if column in df.columns:
                    df[[column, "Cur"]] = df[column].str.extract(
                        r"([\d.,]+)\s*([A-Z]{3})?", expand=True
                    )
                    df[column] = pd.to_numeric(
                        df[column].str.replace(",", ""), errors="coerce"
                    )
                    df["Cur"] = df["Cur"].replace("", pd.NA)
                    df["Cur"] = df["Cur"].astype(str).replace("nan", "")
            if "Change %" in df.columns:
                df["Change %"] = df["Change %"].str.replace("−", "-")
                df["Change %"] = (
                    pd.to_numeric(df["Change %"].str.replace("%", ""), errors="coerce")
                    / 100
                )
                df["Change %"] = df["Change %"].fillna(0)
                df.rename(columns={"Change %": "Rt"}, inplace=True)
            df["Tab"] = quote_mappings.get(quote, "Unknown")
            all_data.append(df)
        except Exception as e:
            print(f"Failed: {e}")
            df = pd.DataFrame()
        finally:
            driver.quit()
    final_data = pd.concat(all_data, ignore_index=True)
    print(final_data)
    final_data.to_json(
        f"{tradingview_path}/tradingview_all_indices.json", orient="records", indent=4
    )
    return "Scrape Prices Indices Sucessfully"


if __name__ == "__main__":
    result = download_all_indices_tradingview(quote="")
