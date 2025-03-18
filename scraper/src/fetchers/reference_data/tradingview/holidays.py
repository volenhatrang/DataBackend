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
import pandas_market_calendars as mcal


def download_holiday(
    market=[],
    tradingview_path="/opt/airflow/FinanceDataScraper/database/reference_data/scraping_raw_json/tradingview",
):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-position=-2400,-2400")
    chrome_options.add_argument("start-maximized")
    chrome_options.add_argument("--no-proxy-server")
    chrome_options.add_argument("--force-device-scale-factor=0.9")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
    )

    os.makedirs(tradingview_path, exist_ok=True)

    if market:
        market_list = market
    else:
        with open(
            f"{tradingview_path}/trading_hour.json", "r", encoding="utf-8"
        ) as file:
            data = json.load(file)
        market_list = [item["Market_Symbol"] for item in data]

    fixed_filename = f"{tradingview_path}/holidays.json"
    if os.path.exists(fixed_filename):
        old_data = pd.read_json(fixed_filename)
        existing_markets = set(old_data["Market"].unique())
        market_list = list(set(market_list) - existing_markets)

    all_data = []
    for market in market_list:
        try:
            s = Service()
            driver = webdriver.Chrome(service=s, options=chrome_options)
            driver.set_window_size(1920, 1080)
            driver.get(f"https://www.tradinghours.com/markets/{market.lower()}/")
            wait = WebDriverWait(driver, 10)
            try:
                holidays_tab = wait.until(
                    EC.element_to_be_clickable((By.ID, "holidays-nav-list-tab"))
                )
                driver.execute_script("arguments[0].click();", holidays_tab)
            except Exception as e:
                print(
                    f"Market {market.upper()}: Not found tab holidays - Passed. Failed"
                )
                driver.quit()
                continue

            table = driver.find_element(By.CSS_SELECTOR, "div#holidays-nav-list table")

            headers = [th.text.strip() for th in table.find_elements(By.TAG_NAME, "th")]
            rows_data = []
            rows = table.find_elements(By.TAG_NAME, "tr")
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                cell_texts = [cell.text for cell in cells]
                if len(cell_texts) >= 2:
                    rows_data.append(cell_texts[:2])

            holidays = headers[3:]

            data = []
            for holiday, row in zip(holidays, rows_data):
                date_str, status = row

                date_match = re.search(r"(\w+day), (\w+) (\d+), (\d+)", date_str)
                if date_match:
                    month = date_match.group(2)
                    day = date_match.group(3)
                    year = date_match.group(4)
                    month_number = pd.Timestamp(month + " 1").month
                    date_clean = f"{year}-{month_number:02d}-{int(day):02d}"
                else:
                    date_clean = None

                open_local = None
                close_local = None
                status = re.sub(r"[^\x00-\x7F]+", "", status)
                date_str = re.sub(r"[^\x00-\x7F]+", "", date_str)
                if "Open" in status:
                    parts = status.split(", ")
                    if len(parts) > 1:
                        time_range = parts[1].split("\n")[0].strip()
                        time_parts = time_range.split(" - ")
                        if len(time_parts) == 2:
                            open_local, close_local = time_parts
                            open_local = datetime.strptime(
                                open_local.strip(), "%I:%M %p"
                            ).strftime("%H:%M")
                            close_local = datetime.strptime(
                                close_local.strip(), "%I:%M %p"
                            ).strftime("%H:%M")

                data.append(
                    {
                        "Market": market.upper(),
                        "Holiday": holiday,
                        "Long_Date": date_str,
                        "Date": date_clean,
                        "Status": status.replace("\n", ", "),
                        "Open_Local": open_local,
                        "Close_Local": close_local,
                    }
                )

            df = pd.DataFrame(data)
            print(df)
            all_data.append(df)

        except Exception as e:
            print(f"Failed: {e}")
            df = pd.DataFrame()
        finally:
            driver.quit()
    final_data = pd.concat(all_data, ignore_index=True)
    final_data["Date"] = final_data["Date"].astype(str)
    final_data["Date"] = pd.to_datetime(
        final_data["Date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{tradingview_path}/holidays_{timestamp}.json"

    fixed_filename = f"{tradingview_path}/holidays.json"
    if os.path.exists(fixed_filename):
        old_data = pd.read_json(fixed_filename)
        old_data["Date"] = pd.to_datetime(
            old_data["Date"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

        final_data = pd.concat([old_data, final_data], ignore_index=True)
    final_data = final_data.drop_duplicates(subset=["Market", "Date"])

    if not final_data.empty:
        final_data.to_json(filename, orient="records", indent=4)
        final_data.to_json(fixed_filename, orient="records", indent=4)
        return "Scrape Holiday Sucessfully"
    else:
        return None


def get_full_market_calendar(exchange, year=None):
    calendar = mcal.get_calendar(exchange)
    tz = timezone(str(calendar.tz))

    if year is None:
        start_date = (
            calendar.valid_days(start_date="1900-01-01", end_date="2100-12-31")
            .min()
            .strftime("%Y-%m-%d")
        )
        end_date = (
            calendar.valid_days(start_date="1900-01-01", end_date="2100-12-31")
            .max()
            .strftime("%Y-%m-%d")
        )
        year_str = "all"
    else:
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        year_str = str(year)

    schedule = calendar.schedule(start_date=start_date, end_date=end_date)
    holidays = [
        pd.Timestamp(h).to_pydatetime().strftime("%Y-%m-%d")
        for h in calendar.holidays().holidays
        if h >= pd.Timestamp(start_date) and h <= pd.Timestamp(end_date)
    ]

    partial_days = []
    regular_hours = None
    regular_duration = None

    if not schedule.empty:
        first_full_day = schedule.dropna().iloc[0]
        regular_open = first_full_day["market_open"]
        regular_close = first_full_day["market_close"]
        regular_duration = regular_close - regular_open
        regular_hours = {
            "open_time": regular_open.strftime("%H:%M:%S"),
            "close_time": regular_close.strftime("%H:%M:%S"),
        }

    for date, row in schedule.iterrows():
        if pd.notna(row["market_open"]) and pd.notna(row["market_close"]):
            open_time_dt = row["market_open"]
            close_time_dt = row["market_close"]
            duration = close_time_dt - open_time_dt

            open_time = open_time_dt.strftime("%H:%M:%S")
            close_time = close_time_dt.strftime("%H:%M:%S")

            local_date = tz.localize(pd.Timestamp(date).to_pydatetime())
            is_dst = local_date.dst() != pd.Timedelta(0)

            if regular_duration and duration != regular_duration:
                partial_days.append(
                    {
                        "date": date.strftime("%Y-%m-%d"),
                        "open_time": open_time,
                        "close_time": close_time,
                        "duration_hours": duration.total_seconds() / 3600,
                    }
                )

    market_info = {
        "exchange": exchange,
        "timezone": str(calendar.tz),
        "regular_trading_hours": regular_hours,
        "regular_duration_hours": (
            regular_duration.total_seconds() / 3600 if regular_duration else None
        ),
        "holidays": holidays,
        "partial_trading_days": partial_days,
        "total_trading_days": len(schedule),
        "period": {"start_date": start_date, "end_date": end_date},
    }
    print(market_info)
    # filename = f"holidays_{exchange}_{year_str}.json"
    # file_path = os.path.join(save_path, filename)
    # os.makedirs(save_path, exist_ok=True)

    # with open(file_path, "w", encoding="utf-8") as f:
    #     json.dump(market_info, f, indent=4)

    # print(f"Saved to: {file_path}")
    return market_info


# if __name__ == "__main__":
#     # download_holiday(tradingview_path = "FinanceDataScraper/database/reference_data/scraping_raw_json/tradingview")
#     tradingview_path = (
#         "scraper/src/database/reference_data/scraping_raw_json/tradingview"
#     )
#     json_file_path = f"{tradingview_path}/list_markets.json"

#     try:
#         with open(json_file_path, "r", encoding="utf-8") as f:
#             json_data = json.load(f)
#     except FileNotFoundError:
#         print(f"Error: File {json_file_path} not found!")
#         exit(1)
#     except json.JSONDecodeError:
#         print(f"Error: File {json_file_path} is not a valid JSON!")
#         exit(1)

#     results = []
#     year = datetime.now().year

#     for exchange in json_data["calendars"]:
#         try:
#             print(f"Processing {exchange}...")
#             market_info = get_full_market_calendar(exchange, year=year)
#             results.append({"exchange": exchange, "data": market_info})
#         except Exception as e:
#             print(f"Error processing {exchange}: {str(e)}")

#     output_file = f"{tradingview_path}/holidays_all_market_{year}.json"
#     with open(output_file, "w", encoding="utf-8") as f:
#         json.dump(results, f, indent=4)

#     print(f"Done processing all calendars! Results saved to {output_file}")
