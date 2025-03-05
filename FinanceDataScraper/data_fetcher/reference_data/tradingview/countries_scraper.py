# FINANCEDATASCRAPER/data_fetcher/tradingview/countries_scraper.py
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import json
import os
import datetime


def countries_scraper(tradingview_path = "/opt/airflow/FinanceDataScraper/database/reference_data/scraping_raw_json/tradingview"):
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    # options.add_argument("--disable-gpu")
    # options.add_argument("--no-sandbox")
    # options.add_argument("--disable-dev-shm-usage")
    # options.add_argument("--window-size=1920,1080")
    # options.add_argument("--disable-software-rasterizer")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    URL = "https://www.tradingview.com/data-coverage/"
    driver.get(URL)

    button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "Selectcountry")))
    button.click()

    dialog = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-name="country-select-dialog"]'))
    )

    regions = dialog.find_elements(By.CLASS_NAME, "groupTitle-TiHRzx3B")
    data = []

    for region in regions:
        region_name = region.text.strip()
        countries_list = []

        try:
            container = WebDriverWait(region, 5).until(
                EC.presence_of_element_located(
                    (By.XPATH, "./following-sibling::div[contains(@class, 'marketItemsContainer-TiHRzx3B')]")
                )
            )

            countries = container.find_elements(
                By.XPATH, ".//div[contains(@class, 'iconColor-XLXs8O7w wrapTablet-XLXs8O7w')]"
            )

            for country in countries:
                country_name = country.find_element(By.CLASS_NAME, "title-XLXs8O7w").text.strip()
                country_flag = country.find_element(By.TAG_NAME, "img").get_attribute("src")
                data_market = country.get_attribute("data-market")

                countries_list.append({
                    "country": country_name,
                    "data_market": data_market,
                    "country_flag": country_flag
                })

        except Exception as e:
            print(f"No Country in {region_name}: {e}")

        if countries_list:
            data.append({
                "region": region_name,
                "countries": countries_list
            })

    
    os.makedirs(tradingview_path, exist_ok=True)

    if not countries_list:
        driver.exit()
        return ("Error: No country has scrapped")
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    filename = f"{tradingview_path}/countries_{timestamp}.json"

    json_output = json.dumps(data, indent=4, ensure_ascii=False)

    with open(filename, "w", encoding="utf-8") as f:
        f.write(json_output)

    driver.quit()
    return "Countries scraped successfully!"


if __name__ == "__main__":
    countries_scraper()