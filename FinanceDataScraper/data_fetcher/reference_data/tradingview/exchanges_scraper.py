from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import json
import time
import os
import datetime

def exchanges_scraper(tradingview_path = "/opt/airflow/FinanaceDataScraper/database/reference_data/scraping_raw_json/tradingview"):
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=2560,1440")  
    options.add_argument("--disable-gpu")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    URL = "https://www.tradingview.com/data-coverage/"
    driver.get(URL)

    os.makedirs(tradingview_path, exist_ok=True)

    tabs = ["Popular", "Stocks& Indices", "Futures", "Forex", "Crypto"]
    exchange_data = []

    for tab_id in tabs:
        try:
            print(f"Processing tab: {tab_id}")
            tab_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, f"//button[@id='{tab_id}']"))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", tab_button)
            time.sleep(4) 
            driver.execute_script("arguments[0].click();", tab_button)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "table-qJcpoITA"))
            )
            try:
                button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "showLaptop-qJcpoITA"))
                )
                driver.execute_script("arguments[0].click();", button)
                time.sleep(4)  
            except:
                print(f"No 'Show more' button found for tab {tab_id}")

            table_rows = driver.find_elements(By.CSS_SELECTOR, "table.table-qJcpoITA tbody tr")
            print(f"Number of rows in table for tab {tab_id}: {len(table_rows)}")

            exchange_elements = driver.find_elements(By.CLASS_NAME, "rowWrap-qJcpoITA")
            for element in exchange_elements:
                exchange_name = ""
                try:
                    exchange_name_elem = element.find_element(By.XPATH, ".//span[@class='exchangeName-qJcpoITA']")
                    exchange_name = exchange_name_elem.text
                    if any(ex["exchangeName"] == exchange_name for ex in exchange_data):
                        continue
                except:
                    continue
                exchange_desc_name = ""
                try:
                    exchange_desc_name = element.find_element(By.XPATH, ".//span[@class='exchangeDescName-qJcpoITA']").text
                except:
                    pass
                country = ""
                try:
                    _country_elements = element.find_elements(By.CLASS_NAME, "cell-qJcpoITA")
                    if _country_elements:
                        _country_text = _country_elements[0].find_elements(By.TAG_NAME, "span")[-1].text
                        if _country_text not in ["CURRENCY", "SPOT", "INDICES", "SWAP", "FUTURES", "FUNDAMENTAL"]:
                            country = _country_text
                except:
                    pass

                types = []
                try:
                    types = [badge.text for badge in element.find_elements(By.XPATH, ".//span[@class='content-PlSmolIm']")]
                except:
                    pass
                exchange_data.append({
                    "exchangeName": exchange_name,
                    "exchangeDescName": exchange_desc_name,
                    "country": country,
                    "types": types,
                    "tab": tab_id
                })

        except Exception as e:
            print(f"Error processing tab {tab_id}: {str(e)}")
            continue

    if not exchange_data:
        driver.quit()
        return "Error: No exchange data scraped to save!"

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{tradingview_path}/exchanges_{timestamp}.json"
    json_output = json.dumps(exchange_data, indent=4, ensure_ascii=False)
    with open(filename, "w", encoding="utf-8") as f:
        f.write(json_output)

    driver.quit()
    return "Exchanges scraped successfully!"

if __name__ == "__main__":
    result = exchanges_scraper()
    print(result)