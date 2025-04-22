import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))

from common_fn import *

# doc
# https://investpy.readthedocs.io/_api/stocks.html

# import investpy
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd
import time
from selenium.common.exceptions import TimeoutException
from lxml import etree
from bs4 import BeautifulSoup
import os
from selenium.webdriver.chrome.service import Service
import random
from selenium.webdriver.common.keys import Keys
import polars as pl
from datetime import timedelta, datetime


def convert_number(value):
    if pd.isna(value) or value is None:
        return None
    try:
        if "B" in value:
            return float(value.replace("B", "").replace(",", "")) * 1e9
        elif "M" in value:
            return float(value.replace("M", "").replace(",", "")) * 1e6
        elif "K" in value:
            return float(value.replace("K", "").replace(",", "")) * 1e3
        elif "T" in value:
            return float(value.replace("T", "").replace(",", "")) * 1e12
        else:
            return float(value.replace(",", ""))
    except (ValueError, AttributeError):
        return None


def close_login_popup(driver):
    try:
        pop_up = WebDriverWait(driver, 3).until(
            EC.presence_of_element_located(
                (
                    By.CSS_SELECTOR,
                    "#\:r9\: > div > div.hidden.w-\[360px\].min-w-\[360px\].flex-col.justify-center.rounded-s-md.bg-cover.bg-center.p-12.text-white.md\:flex.rtl\:scale-x-\[-1\]",
                )
            )
        )

        if pop_up.is_displayed():
            print("🔴 Login pop up detected! Closing...")
            actions = ActionChains(driver)
            actions.move_by_offset(0, 0).click().perform()
            WebDriverWait(driver, 2).until(EC.invisibility_of_element(pop_up))
    except:
        pass


def download_inv_prices_by_code(pCode="equities/a.j.-plast", number_day=100000):

    all_data = []
    end_date = datetime.today().strftime("%Y-%m-%d")
    remaining_days = number_day

    while remaining_days > 5:
        start_date = (datetime.today() - timedelta(days=remaining_days)).strftime(
            "%Y-%m-%d"
        )

        print(f"🔄 Download data from {start_date} to {end_date}")
        url = f"https://www.investing.com/{pCode}-historical-data"
        chrome_options = Options()
        # chrome_options.add_argument(
        #     "--headless=new"
        # )
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--force-device-scale-factor=0.9")
        chrome_options.add_argument(
            "--disable-blink-features=AutomationControlled"
        )  # Avoid bot detection
        chrome_options.add_argument("--window-position=-2400,-2400")

        driver = webdriver.Chrome(service=Service(), options=chrome_options)
        driver.get(url)
        driver.implicitly_wait(5)

        actions = ActionChains(driver)
        actions.move_by_offset(0, 0).click().perform()

        try:
            close_login_popup(driver)

            calendar_button = driver.find_element(
                By.CSS_SELECTOR,
                "#__next > div.md\:relative.md\:bg-white > div.relative.flex > div.md\:grid-cols-\[1fr_72px\].md2\:grid-cols-\[1fr_420px\].grid.flex-1.grid-cols-1.px-4.pt-5.font-sans-v2.text-\[\#232526\].antialiased.transition-all.xl\:container.sm\:px-6.md\:gap-6.md\:px-7.md\:pt-10.md2\:gap-8.md2\:px-8.xl\:mx-auto.xl\:gap-10.xl\:px-10 > div.min-w-0 > div.mb-4.md\:mb-10 > div.sm\:flex.sm\:items-end.sm\:justify-between > div.relative.flex.items-center.md\:gap-6 > div.flex.flex-1.items-center.gap-3\.5.rounded.border.border-solid.border-\[\#CFD4DA\].bg-white.px-3\.5.py-2.shadow-select > div",
            )
            calendar_button.click()

            input_element = driver.find_element(
                By.CSS_SELECTOR,
                "#__next > div.md\:relative.md\:bg-white > div.relative.flex > div.md\:grid-cols-\[1fr_72px\].md2\:grid-cols-\[1fr_420px\].grid.flex-1.grid-cols-1.px-4.pt-5.font-sans-v2.text-\[\#232526\].antialiased.transition-all.xl\:container.sm\:px-6.md\:gap-6.md\:px-7.md\:pt-10.md2\:gap-8.md2\:px-8.xl\:mx-auto.xl\:gap-10.xl\:px-10 > div.min-w-0 > div.mb-4.md\:mb-10 > div.sm\:flex.sm\:items-end.sm\:justify-between > div.relative.flex.items-center.md\:gap-6 > div.absolute.right-0.top-\[42px\].z-5.inline-flex.flex-col.items-end.gap-4.rounded.border.border-solid.bg-white.p-4.shadow-secondary > div.flex.items-start.gap-3 > div:nth-child(1) > input",
            )
            input_element.clear()
            input_element.send_keys(start_date)
            input_element.send_keys(Keys.RETURN)

            apply_button = driver.find_element(
                By.CSS_SELECTOR,
                "#__next > div.md\:relative.md\:bg-white > div.relative.flex > div.md\:grid-cols-\[1fr_72px\].md2\:grid-cols-\[1fr_420px\].grid.flex-1.grid-cols-1.px-4.pt-5.font-sans-v2.text-\[\#232526\].antialiased.transition-all.xl\:container.sm\:px-6.md\:gap-6.md\:px-7.md\:pt-10.md2\:gap-8.md2\:px-8.xl\:mx-auto.xl\:gap-10.xl\:px-10 > div.min-w-0 > div.mb-4.md\:mb-10 > div.sm\:flex.sm\:items-end.sm\:justify-between > div.relative.flex.items-center.md\:gap-6 > div.absolute.right-0.top-\[42px\].z-5.inline-flex.flex-col.items-end.gap-4.rounded.border.border-solid.bg-white.p-4.shadow-secondary > div.flex.cursor-pointer.items-center.gap-3.rounded.bg-v2-blue.py-2\.5.pl-4.pr-6.shadow-button.hover\:bg-\[\#116BCC\]",
            )
            apply_button.click()
            close_login_popup(driver)

            time.sleep(2)
        except Exception as e:
            print(f"⚠️ Failed when choose date: {e}")
            break

        try:
            table_element = driver.find_element(
                By.CSS_SELECTOR,
                "#__next > div.md\:relative.md\:bg-white > div.relative.flex > div.md\:grid-cols-\[1fr_72px\].md2\:grid-cols-\[1fr_420px\].grid.flex-1.grid-cols-1.px-4.pt-5.font-sans-v2.text-\[\#232526\].antialiased.transition-all.xl\:container.sm\:px-6.md\:gap-6.md\:px-7.md\:pt-10.md2\:gap-8.md2\:px-8.xl\:mx-auto.xl\:gap-10.xl\:px-10 > div.min-w-0 > div.mb-4.md\:mb-10 > div.mt-6.flex.flex-col.items-start.overflow-x-auto.p-0.md\:pl-1 > table",
            )
            headers = [
                th.text.strip() for th in table_element.find_elements(By.TAG_NAME, "th")
            ]
            table_data = driver.execute_script(
                """
                let table = arguments[0];
                let rows = table.querySelectorAll("tr");
                return Array.from(rows).slice(1).map(row =>
                    Array.from(row.querySelectorAll("td")).map(td => td.innerText)
                );
                """,
                table_element,
            )

            df = pl.DataFrame(table_data, schema=headers)
            df = df.with_columns(
                pl.col("Date").str.strptime(pl.Date, format="%b %d, %Y").cast(pl.Utf8)
            )
            df = df.rename({"Vol.": "Volume", "Change %": "rt"})
            df = df.with_columns(
                pl.col("rt").str.replace("%", "").cast(pl.Float64) / 100
            )
            df = df.with_columns(
                pl.col("Volume").map_elements(convert_number, return_dtype=pl.Float64)
            )

            all_data.append(df)
        except Exception as e:
            print(f"❌ Failed when get data: {e}")
            break

        if df.is_empty():
            break

        max_date = df.select(pl.col("Date").max())[0, 0]
        print(f"✅ Max date of downloaded data: {max_date}")

        if max_date:
            max_date_dt = datetime.strptime(max_date, "%Y-%m-%d")
            remaining_days = (datetime.today() - max_date_dt).days
        else:
            break

        driver.quit()

    if all_data:
        full_df = pl.concat(all_data)
        full_df = full_df.sort("Date")
        print(f"✅ Full Data: {full_df.shape[0]} rows")
        print(full_df)
        return full_df
    else:
        print("⚠️ No Data")
        return None


def search_inv_codesource_by_string(text="AAPL"):
    result_link = ""
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--window-size=1920x1080")
    # chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--window-position=-2400,-2400")

    chrome_options.add_argument("--force-device-scale-factor=0.9")
    driver = webdriver.Chrome(service=Service(), options=chrome_options)

    driver.get("https://www.investing.com/")
    close_login_popup(driver)
    actions = ActionChains(driver)
    actions.move_by_offset(0, 0).click().perform()
    search_box = driver.find_element(
        By.CSS_SELECTOR,
        "#__next > header > div.flex.justify-center.xxl\:px-\[160px\].xxxl\:px-\[300px\].header_top-row-wrapper__7SAiJ > section > div.mainSearch_mainSearch__jEh4W.p-2\.5.md\:\!mx-8.md\:p-0.md2\:relative.md2\:\!mx-12.lg\:\!mx-20 > div.flex.mainSearch_search-bar____mI1 > div > form > input",
    )
    search_box.send_keys(text)
    time.sleep(2)
    search_box.click()
    first_result = driver.find_element(
        By.CSS_SELECTOR,
        "#__next > header > div.flex.justify-center.xxl\:px-\[160px\].xxxl\:px-\[300px\].header_top-row-wrapper__7SAiJ > section > div.mainSearch_mainSearch__jEh4W.p-2\.5.md\:\!mx-8.md\:p-0.md2\:relative.md2\:\!mx-12.lg\:\!mx-20 > div.flex.md\:\!left-6.md2\:\!left-0.md2\:\!top-\[calc\(100\%_\+_6px\)\].mainSearch_mainSearch_results__pGhOQ > div.mainSearch_main__exqg8 > div > div > ul > li:nth-child(1) > a",
    )
    result_link = first_result.get_attribute("href")
    print(result_link)
    driver.quit()
    return result_link


def download_inv_shares_capi_by_code(pCode="equities/apple-computer-inc"):

    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    # chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--force-device-scale-factor=0.9")
    chrome_options.add_argument("--window-position=-2400,-2400")

    driver = webdriver.Chrome(service=Service(), options=chrome_options)

    url = f"https://www.investing.com/{pCode}"
    driver.get(url)
    df = pd.DataFrame()
    driver.implicitly_wait(5)
    close_login_popup(driver)
    actions = ActionChains(driver)
    actions.move_by_offset(0, 0).click().perform()

    try:
        actions.move_by_offset(0, 0).click().perform()
        divs = driver.find_elements(
            "css selector",
            ".flex.flex-wrap.items-center.justify-between.border-t.border-t-\\[\\#e6e9eb\\].pt-2\\.5.text-xs.leading-4.sm\\:pb-2\\.5.pb-2\\.5",
        )

        texts = [div.text for div in divs]
        cleaned_data = [t.split("\n") for t in texts if t.strip()]

        data_dict = {}
        for item in cleaned_data:
            if len(item) > 2:
                key = item[0]
                value = " ".join(item[1:])
            else:
                key, value = item if len(item) == 2 else (item[0], None)
            data_dict[key] = value

        df = pl.DataFrame([data_dict])[["Market Cap", "Shares Outstanding"]]
        df = df.with_columns(
            [df[col].map_elements(convert_number).alias(col) for col in df.columns]
        )

        df = df.with_columns(pl.lit(pCode).alias("codesource"))

        actions.move_by_offset(0, 0).click().perform()

        print(df)
    except Exception as e:
        print(f"❌ Error: {e}")

    driver.quit()
    return df


def download_inv_profile_by_code(pCode="equities/apple-computer-inc"):

    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    # chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--force-device-scale-factor=0.9")
    chrome_options.add_argument("--window-position=-2400,-2400")

    driver = webdriver.Chrome(service=Service(), options=chrome_options)

    url = f"https://www.investing.com/{pCode}-company-profile"
    driver.get(url)
    df = pd.DataFrame()
    driver.implicitly_wait(5)
    close_login_popup(driver)
    actions = ActionChains(driver)
    actions.move_by_offset(0, 0).click().perform()

    try:
        actions.move_by_offset(0, 0).click().perform()
        company_address = driver.find_elements(
            By.CSS_SELECTOR,
            "#leftColumn > div.companyProfileContactInfo > div.info.float_lang_base_1 > div.companyAddress > span.float_lang_base_2.text_align_lang_base_2.dirLtr",
        )
        if company_address:
            address_text = company_address[0].get_attribute("innerText")
            address_text = " ".join(address_text.splitlines())

        company_phone = driver.find_elements(
            By.CSS_SELECTOR,
            "#leftColumn > div.companyProfileContactInfo > div.info.float_lang_base_1 > div.companyPhone > span.float_lang_base_2.text_align_lang_base_2.dirLtr",
        )

        if company_phone:
            phone_text = company_phone[0].get_attribute("innerText")
            phone_text = " ".join(phone_text.splitlines())

        company_fax = driver.find_elements(
            By.CSS_SELECTOR,
            "#leftColumn > div.companyProfileContactInfo > div.info.float_lang_base_1 > div.companyFax > span.float_lang_base_2.text_align_lang_base_2.dirLtr",
        )
        if company_fax:
            fax_text = company_fax[0].get_attribute("innerText")
            fax_text = " ".join(fax_text.splitlines())

        company_web = driver.find_elements(
            By.CSS_SELECTOR,
            "#leftColumn > div.companyProfileContactInfo > div.info.float_lang_base_1 > div.companyWeb > span.float_lang_base_2.text_align_lang_base_2.dirLtr",
        )
        if company_web:
            web_text = company_web[0].get_attribute("innerText")
            web_text = " ".join(web_text.splitlines())

        actions.move_by_offset(0, 0).click().perform()

        company_board = driver.find_elements(By.CSS_SELECTOR, "#leftColumn > table")

        headers = [
            th.text.strip() for th in company_board[0].find_elements(By.TAG_NAME, "th")
        ]

        table_data = driver.execute_script(
            """
            let table = arguments[0];
            let rows = table.querySelectorAll("tr");
            return Array.from(rows).slice(1).map(row =>
                Array.from(row.querySelectorAll("td")).map(td => td.innerText)
            );
            """,
            company_board[0],
        )

        df = pl.DataFrame(table_data, schema=headers)
        df = df.with_columns(pl.lit(address_text).alias("address"))
        df = df.with_columns(pl.lit(fax_text).alias("fax"))
        df = df.with_columns(pl.lit(phone_text).alias("phone"))
        df = df.with_columns(pl.lit(web_text).alias("web"))
        df = df.with_columns(pl.lit(pCode).alias("codesource"))

        print(df)
    except Exception as e:
        print(f"❌ Error: {e}")

    driver.quit()
    return df


# x = investpy.stocks.get_stocks(country=None)
x = investpy.stocks.get_stock_countries()
exchanges = investpy.get_stocks(country="vietnam")

company_profile = investpy.get_stock_company_profile(
    stock="bbva", country="spain", language="english"
)
# x.to_csv("/home/baongoc2001/project/Bigdata-System/airflow/data/list_crypto_inv.csv")
print(exchanges)
