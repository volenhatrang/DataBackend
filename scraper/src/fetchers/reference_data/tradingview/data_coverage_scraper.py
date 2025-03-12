from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import json
import time
import os
from dotenv import load_dotenv
import datetime

def setup_driver():
    """Initialize and return a Selenium WebDriver instance."""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=2560,1440")
    options.add_argument("--disable-gpu")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def countries_scraper(tradingview_path="/opt/airflow/FinanceDataScraper/database/reference_data/scraping_raw_json/tradingview"):

    driver = setup_driver()
    URL = "https://www.tradingview.com/data-coverage/"
    print(f"Navigating to {URL}")
    driver.get(URL)

    os.makedirs(tradingview_path, exist_ok=True)

    try:
        print("Waiting for 'Selectcountry' button...")
        button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "Selectcountry"))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", button)
        time.sleep(2)  
        driver.execute_script("arguments[0].click();", button)

        print("Waiting for country select dialog...")
        dialog = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-name="country-select-dialog"]'))
        )

        regions = dialog.find_elements(By.CLASS_NAME, "groupTitle-TiHRzx3B")
        print(f"Found {len(regions)} regions to process")
        data = []

        for region in regions:
            region_name = region.text.strip()
            print(f"Processing region: {region_name}")
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
                print(f"Found {len(countries)} countries in {region_name}")

                for country in countries:
                    try:
                        country_name = country.find_element(By.CLASS_NAME, "title-XLXs8O7w").text.strip()
                        country_flag = country.find_element(By.TAG_NAME, "img").get_attribute("src")
                        data_market = country.get_attribute("data-market")

                        countries_list.append({
                            "country": country_name,
                            "data_market": data_market,
                            "country_flag": country_flag
                        })
                    except Exception as e:
                        print(f"Error processing country in {region_name}: {e}")
                        continue

            except Exception as e:
                print(f"No countries found in {region_name}: {e}")

            if countries_list:
                data.append({
                    "region": region_name,
                    "countries": countries_list
                })

        if not data:
            driver.quit()
            return "Error: No country data scraped to save!"

        timestamp = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=7))).strftime("%Y%m%d_%H%M%S")
        filename = f"{tradingview_path}/countries_{timestamp}.json"
        json_output = json.dumps(data, indent=4, ensure_ascii=False)

        with open(filename, "w", encoding="utf-8") as f:
            f.write(json_output)
        print(f"Data saved to {filename}")

        driver.quit()
        return "Countries scraped successfully!"

    except Exception as e:
        print(f"Error during scraping: {e}")
        driver.quit()
        return f"Error: Scraping failed - {str(e)}"


def scrape_tab(driver, tab_id, selector, is_economy=False):
    """Scrape data from a specific tab and return a list of exchange data."""
    print(f"Processing tab: {tab_id}")
    exchange_data = []

    try:
        tab_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, f"//button[@id='{tab_id}']"))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", tab_button)
        time.sleep(1)  
        driver.execute_script("arguments[0].click();", tab_button)
        time.sleep(1)  

        try:
            button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "showLaptop-qJcpoITA"))
            )
            driver.execute_script("arguments[0].click();", button)
            time.sleep(1)
        except:
            print(f"No 'Show more' button found for tab {tab_id}")

        exchange_elements = driver.find_elements(By.CSS_SELECTOR, selector)
        print(f"Found {len(exchange_elements)} elements in {tab_id}")

        for element in exchange_elements:
            exchange_name = ""
            try:
                name_elem = element.find_elements(By.CSS_SELECTOR, ".exchangeName-qJcpoITA")
                if name_elem:
                    exchange_name = name_elem[0].text
                    if any(ex["exchangeName"] == exchange_name for ex in exchange_data):
                        continue
            except:
                continue

            exchange_desc_name = ""
            try:
                desc_elem = element.find_elements(By.CSS_SELECTOR, ".exchangeDescName-qJcpoITA")
                if desc_elem:
                    exchange_desc_name = desc_elem[0].text
            except:
                pass

            country = ""
            is_world = False
            if is_economy:
                try:
                    country_elem = element.find_elements(By.CSS_SELECTOR, ".exchangeDescription-qJcpoITA span")
                    if country_elem:
                        country = country_elem[0].text
                        is_world = (country == "World")
                except:
                    pass
            else:
                try:
                    cell_elem = element.find_elements(By.CLASS_NAME, "cell-qJcpoITA")
                    if cell_elem:
                        country_text = cell_elem[0].find_elements(By.TAG_NAME, "span")[-1].text
                        if country_text not in ["CURRENCY", "SPOT", "INDICES", "SWAP", "FUTURES", "FUNDAMENTAL"]:
                            country = country_text
                except:
                    pass

            types = []
            try:
                types = [badge.text for badge in element.find_elements(By.CSS_SELECTOR, ".content-PlSmolIm")]
            except:
                pass

            exchange_data.append({
                "exchangeName": exchange_name,
                "exchangeDescName": exchange_desc_name if not is_economy else "",
                "country": country,
                "is_world": is_world if is_economy else False,
                "types": types,
                "tab": tab_id
            })

    except Exception as e:
        print(f"Error processing tab {tab_id}: {str(e)}")

    return exchange_data

def save_to_json(data, filepath):
    """Save data to a JSON file."""
    if not data:
        print(f"No data to save for {filepath}")
        return False
    json_output = json.dumps(data, indent=4, ensure_ascii=False)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(json_output)
    print(f"Saved data to {filepath}")
    return True

def crawler_data_coverage(tradingview_path = "/opt/airflow/FinanaceDataScraper/database/reference_data/scraping_raw_json/tradingview" ):
    """Main function to crawl data coverage from TradingView."""

    driver = setup_driver()
    URL = "https://www.tradingview.com/data-coverage/"
    print(f"Navigating to {URL}")
    driver.get(URL)
    tab_configs = [
        {"id": "Popular", "selector": "#tab-region-Popular tbody tr", "filename": "exchanges-popular"},
        {"id": "Stocks& Indices", "selector": ".rowWrap-qJcpoITA", "filename": "exchanges-stocks-indices"},
        {"id": "Futures", "selector": ".rowWrap-qJcpoITA", "filename": "exchanges-futures"},
        {"id": "Forex", "selector": "#tab-region-Forex tbody tr", "filename": "exchanges-forex"},
        {"id": "Crypto", "selector": "#tab-region-Crypto tbody tr", "filename": "exchanges-crypto"},
        {"id": "Economy", "selector": "#tab-region-Economy .economyTableRow-qJcpoITA", "filename": "exchanges-economy", "is_economy": True}
    ]

    timestamp = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=7))).strftime("%Y%m%d_%H%M%S")
    os.makedirs(tradingview_path, exist_ok=True)

    for config in tab_configs:
        data = scrape_tab(driver, config["id"], config["selector"], config.get("is_economy", False))
        filename = f"{tradingview_path}/{config['filename']}_{timestamp}.json"
        save_to_json(data, filename)

    driver.quit()
    print("Crawling completed successfully!")


def countries_scraper(tradingview_path="/opt/airflow/FinanceDataScraper/database/reference_data/scraping_raw_json/tradingview"):

    driver = setup_driver()
    URL = "https://www.tradingview.com/data-coverage/"
    print(f"Navigating to {URL}")
    driver.get(URL)

    os.makedirs(tradingview_path, exist_ok=True)

    try:
        print("Waiting for 'Selectcountry' button...")
        button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "Selectcountry"))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", button)
        time.sleep(2)  
        driver.execute_script("arguments[0].click();", button)

        print("Waiting for country select dialog...")
        dialog = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-name="country-select-dialog"]'))
        )

        regions = dialog.find_elements(By.CLASS_NAME, "groupTitle-TiHRzx3B")
        print(f"Found {len(regions)} regions to process")
        data = []

        for region in regions:
            region_name = region.text.strip()
            print(f"Processing region: {region_name}")
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
                print(f"Found {len(countries)} countries in {region_name}")

                for country in countries:
                    try:
                        country_name = country.find_element(By.CLASS_NAME, "title-XLXs8O7w").text.strip()
                        country_flag = country.find_element(By.TAG_NAME, "img").get_attribute("src")
                        data_market = country.get_attribute("data-market")

                        countries_list.append({
                            "country": country_name,
                            "data_market": data_market,
                            "country_flag": country_flag
                        })
                    except Exception as e:
                        print(f"Error processing country in {region_name}: {e}")
                        continue

            except Exception as e:
                print(f"No countries found in {region_name}: {e}")

            if countries_list:
                data.append({
                    "region": region_name,
                    "countries": countries_list
                })

        if not data:
            driver.quit()
            return "Error: No country data scraped to save!"

        timestamp = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=7))).strftime("%Y%m%d_%H%M%S")
        filename = f"{tradingview_path}/countries_{timestamp}.json"
        json_output = json.dumps(data, indent=4, ensure_ascii=False)

        with open(filename, "w", encoding="utf-8") as f:
            f.write(json_output)
        print(f"Data saved to {filename}")

        driver.quit()
        return "Countries scraped successfully!"

    except Exception as e:
        print(f"Error during scraping: {e}")
        driver.quit()
        return f"Error: Scraping failed - {str(e)}"


if __name__ == "__main__":
    try:
        crawler_data_coverage(tradingview_path = "/opt/airflow/FinanaceDataScraper/database/reference_data/scraping_raw_json/tradingview")
    except Exception as e:
        print(f"Failed to complete crawling: {str(e)}")