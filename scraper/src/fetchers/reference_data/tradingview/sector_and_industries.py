from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import pandas as pd
import re
import os
from datetime import datetime
import json
from pytz import timezone

# import pandas_market_calendars as mcal
from tabulate import tabulate
from bs4 import BeautifulSoup
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
)
import numpy as np

# import polars as pl


def view_data(df):
    if len(df) > 10:
        df_display = df.head(5)._append(df.tail(5))
    else:
        df_display = df
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
                    "",
                    re.sub(r"\*", "", col.lower()),
                ),
            ),
        )  # Remove '*' and make lowercase
        for col in df.columns
    ]
    return df


def convert_number(value="2.92 T USD"):
    if pd.isna(value) or value == "":
        return np.nan, None

    if isinstance(value, (int, float)):
        return value, None

    value = str(value).replace("\u202f", "").replace(" ", "").replace(" ", "").strip()

    match = re.match(r"([\d\.]+)([TMBK]?)\s*([A-Z]*)", value)
    if match:
        number = float(match.group(1))
        unit = match.group(2)
        currency = match.group(3)

        multiplier = {"T": 1e12, "B": 1e9, "M": 1e6, "K": 1e3, "": 1}
        number *= multiplier.get(unit, 1)

        return number, currency
    return np.nan, None


def clean_dash(
    df,
    numeric_columns=[
        "rt",
        "marketcap",
        "price",
        "volume",
        "relvolume",
        "p/e",
        "epsdilttm",
        "epsdilgrowthttmyoy",
        "divyieldttm",
    ],
):
    # df.replace(["—", "-", ""], np.nan, inplace=True)
    df.replace(r"^—$", np.nan, regex=True, inplace=True)
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
            df[col] = df[col].str.replace("−", "-", regex=True)
    return df


def get_tradingview_sectors_by_country(country="usa"):

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
    chrome_options.add_argument(
        "--disable-blink-features=AutomationControlled"
    )  # Avoid bot detection
    url_sector = f"https://www.tradingview.com/markets/stocks-{country}/sectorandindustry-sector/"

    s = Service()
    driver = webdriver.Chrome(service=s, options=chrome_options)
    driver.set_window_size(1920, 1080)
    df = pd.DataFrame()
    try:
        driver.get(url_sector)
        while True:
            try:
                load_more = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            '//*[@id="js-category-content"]/div[2]/div/div[4]/div[3]/button',
                        )
                    )
                )
                if load_more:
                    driver.execute_script(
                        "arguments[0].scrollIntoView(true);", load_more
                    )
                    driver.execute_script("arguments[0].click();", load_more)
            except (
                TimeoutException,
                NoSuchElementException,
                StaleElementReferenceException,
            ):
                print("No more 'Load More' button found. All data should be loaded.")
                break
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
        hrefs = []

        for row in rows:
            row_key = row.get_attribute("data-rowkey")
            if row_key:
                row_keys.append(row_key)
            cells = row.find_elements(By.TAG_NAME, "td")

            link_element = None
            try:
                link_element = cells[0].find_element(By.TAG_NAME, "a")
                href = link_element.get_attribute("href")
                if href:
                    hrefs.append(href)
            except:
                href = None

            cell_data = []
            for cell in cells:
                full_text = cell.text.strip()
                cell_data.append(full_text)
            if any(cell_data):
                cell_data_lists.append(cell_data)

        df = pd.DataFrame(
            cell_data_lists, index=row_keys, columns=column_names
        ).reset_index()
        df["component_url"] = hrefs
        df["country"] = country
        df.rename(columns={"index": "ICB_Code"}, inplace=True)
        df = clean_colnames(df)
        df.rename(columns={"change": "rt"}, inplace=True)

        df[["marketcap", "cur"]] = df["marketcap"].apply(
            lambda x: pd.Series(convert_number(x))
        )

        df["divyield(indicated)"] = df["divyield(indicated)"].replace(
            ["—", "-", ""], np.nan
        )

        df["divyield(indicated)"] = df["divyield(indicated)"].apply(
            lambda x: f"{x}%" if pd.notna(x) and "%" not in x else x
        )

        df["divyield_percent"] = (
            df["divyield(indicated)"].str.replace("%", "", regex=False).astype(float)
            / 100
        )

        df["rt"] = (
            df["rt"].str.replace("%", "").str.replace("−", "-").astype(float) / 100
        )

        df["volume"] = df["volume"].apply(lambda x: pd.Series(convert_number(x)[0]))

        df = df.drop(columns=["divyield(indicated)"])
        driver.quit()
        return df
    except Exception as e:
        print(f"Failed: {e}")
        driver.quit()

        return df


def get_tradingview_industries_by_country(country="usa"):

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
    chrome_options.add_argument(
        "--disable-blink-features=AutomationControlled"
    )  # Avoid bot detection
    url = f"https://www.tradingview.com/markets/stocks-{country}/sectorandindustry-industry/"

    s = Service()
    driver = webdriver.Chrome(service=s, options=chrome_options)
    driver.set_window_size(1920, 1080)
    df = pd.DataFrame()
    try:
        driver.get(url)
        while True:
            try:
                load_more = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable(
                        (
                            By.XPATH,
                            '//*[@id="js-category-content"]/div[2]/div/div[4]/div[3]/button',
                        )
                    )
                )
                if load_more:
                    driver.execute_script(
                        "arguments[0].scrollIntoView(true);", load_more
                    )
                    driver.execute_script("arguments[0].click();", load_more)
            except (
                TimeoutException,
                NoSuchElementException,
                StaleElementReferenceException,
            ):
                print("No more 'Load More' button found. All data should be loaded.")
                break
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
        hrefs = []

        for row in rows:
            row_key = row.get_attribute("data-rowkey")
            if row_key:
                row_keys.append(row_key)
            cells = row.find_elements(By.TAG_NAME, "td")

            link_element = None
            try:
                link_element = cells[0].find_element(By.TAG_NAME, "a")
                href = link_element.get_attribute("href")
                if href:
                    hrefs.append(href)
            except:
                href = None

            cell_data = []
            for cell in cells:
                full_text = cell.text.strip()
                cell_data.append(full_text)
            if any(cell_data):
                cell_data_lists.append(cell_data)

        df = pd.DataFrame(
            cell_data_lists, index=row_keys, columns=column_names
        ).reset_index()
        df["component_url"] = hrefs
        df["country"] = country
        df.rename(columns={"index": "ICB_Code"}, inplace=True)
        df = clean_colnames(df)
        df.rename(columns={"change": "rt"}, inplace=True)

        df[["marketcap", "cur"]] = df["marketcap"].apply(
            lambda x: pd.Series(convert_number(x))
        )

        df["divyield(indicated)"] = df["divyield(indicated)"].replace(
            ["—", "-", ""], np.nan
        )

        df["divyield(indicated)"] = df["divyield(indicated)"].apply(
            lambda x: f"{x}%" if pd.notna(x) and "%" not in x else x
        )

        df["divyield_percent"] = (
            df["divyield(indicated)"].str.replace("%", "", regex=False).astype(float)
            / 100
        )
        df = df.drop(columns=["divyield(indicated)"])

        df["rt"] = (
            df["rt"].str.replace("%", "").str.replace("−", "-").astype(float) / 100
        )

        df["volume"] = df["volume"].apply(lambda x: pd.Series(convert_number(x)[0]))
        driver.quit()

        return df
    except Exception as e:
        print(f"Failed: {e}")
        driver.quit()

        return df


def get_tradingview_sectors_industries_components(
    url="https://www.tradingview.com/markets/stocks-usa/sectorandindustry-sector/technology-services/",
):
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
    chrome_options.add_argument(
        "--disable-blink-features=AutomationControlled"
    )  # Avoid bot detection
    s = Service()
    driver = webdriver.Chrome(service=s, options=chrome_options)
    driver.set_window_size(1920, 1080)
    df = pd.DataFrame()
    try:
        driver.get(url)
        while True:
            try:
                # load_more = WebDriverWait(driver, 5).until(
                #     EC.element_to_be_clickable((By.XPATH, '//*[@id="js-category-content"]/div[2]/div/div[4]/div[3]/button'))
                # )
                # load_more = driver.find_elements(
                #     By.XPATH,
                #     '//*[@id="js-category-content"]/div[2]/div/div[4]/div[3]/button',
                # )
                load_more = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            '//*[@id="js-category-content"]/div[2]/div/div[4]/div[3]/button',
                        )
                    )
                )
                if load_more:
                    driver.execute_script(
                        "arguments[0].scrollIntoView(true);", load_more
                    )
                    driver.execute_script("arguments[0].click();", load_more)
                else:
                    break
            except (
                TimeoutException,
                NoSuchElementException,
                StaleElementReferenceException,
            ):
                print("No more 'Load More' button found. All data should be loaded.")
                break

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
        hrefs = []

        for row in rows:
            row_key = row.get_attribute("data-rowkey")
            if row_key:
                row_keys.append(row_key)
            cells = row.find_elements(By.TAG_NAME, "td")

            link_element = None
            try:
                link_element = cells[0].find_element(By.TAG_NAME, "a")
                href = link_element.get_attribute("href")
                if href:
                    hrefs.append(href)
            except:
                href = None

            cell_data = []
            for idx, cell in enumerate(cells):
                if idx == 0:
                    symbol_text = ""
                    company_text = ""
                    try:
                        ticker_name_element = cell.find_element(
                            By.CSS_SELECTOR, "a.tickerName-GrtoTeat"
                        )
                        symbol_text = ticker_name_element.text.strip()
                    except:
                        pass
                    try:
                        company_element = cell.find_element(
                            By.CSS_SELECTOR, "sup.tickerDescription-GrtoTeat"
                        )
                        company_text = company_element.text.strip()
                    except:
                        pass
                    full_text = "\n".join(
                        filter(None, [symbol_text, company_text])
                    ).strip()
                else:
                    full_text = cell.text.strip()
                cell_data.append(full_text)
            if any(cell_data):
                cell_data_lists.append(cell_data)

        df = pd.DataFrame(
            cell_data_lists, index=row_keys, columns=column_names
        ).reset_index()
        df["stock_url"] = hrefs
        df.rename(columns={"index": "codesource"}, inplace=True)

        split_df = df["Symbol"].str.split("\n", expand=True)
        if split_df.shape[1] < 2:
            split_df = split_df.reindex(columns=range(2), fill_value="")

        df[["ticker", "name"]] = split_df[[0, 1]].fillna("")
        df = df.drop(columns=["Symbol"])
        df = clean_colnames(df)
        df.rename(columns={"change": "rt"}, inplace=True)
        df.rename(columns={"p/e": "pe"}, inplace=True)

        df = clean_dash(df, numeric_columns=["marketcap", "price", "rt", "volume"])

        df[["marketcap"]] = df["marketcap"].apply(
            lambda x: pd.Series(convert_number(x)[0])
        )

        df[["price", "cur"]] = df["price"].apply(lambda x: pd.Series(convert_number(x)))

        df["rt"] = df["rt"].replace(["—", "-", ""], np.nan)
        df["rt"] = df["rt"].apply(
            lambda x: f"{x}%" if pd.notna(x) and "%" not in x else x
        )
        df["rt"] = (
            df["rt"].str.replace("%", "").str.replace("−", "-").astype(float) / 100
        )

        df["volume"] = df["volume"].apply(lambda x: pd.Series(convert_number(x)[0]))
        view_data(df)
        driver.quit()
        return df
    except Exception as e:
        print(f"Failed: {e}")
        driver.quit()
        return df


# if __name__ == "__main__":
#     with open('FinanceDataScraper/database/reference_data/scraping_raw_json/tradingview/countries_with_flags.json', 'r', encoding='utf-8') as file:
#         data = json.load(file)

#     all_countries = []
#     for region in data:
#         countries = [country['country'] for country in region['countries']]
#         all_countries.extend(countries)
