from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import pandas as pd
import os 
import re
import time
from datetime import datetime

def download_trading_hour(tradingview_path = "/opt/airflow/FinanceDataScraper/database/reference_data/scraping_raw_json/tradingview"):
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
    
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    os.makedirs(tradingview_path, exist_ok=True)

    try:
        s = Service()
        driver = webdriver.Chrome(service=s, options=chrome_options)
        driver.set_window_size(1920, 1080)
        driver.get(f"https://www.tradinghours.com/markets/")
        wait = WebDriverWait(driver, 10) 
        
        showmore_tab = wait.until(EC.element_to_be_clickable((By.ID, "show_more")))
        
        driver.execute_script("arguments[0].click();", showmore_tab)
        #main > div.paywall > div > table
        table = driver.find_element(By.CSS_SELECTOR, '#main > div.paywall > div > table')

        rows_data = []
        rows = table.find_elements(By.TAG_NAME, "tr")
        for row in rows:
            cells = row.find_elements(By.TAG_NAME, "td") 
            cell_texts = [cell.text for cell in cells]  
            if len(cell_texts) >= 3:  
                rows_data.append(cell_texts[:2])  
                
        data = []
        for row in rows_data:
            timezone = open_local = close_local = None
            iso2, market_exchange, market_symbol = row[0].split('\n')
            iso2 = iso2.strip()
            market_exchange = market_exchange.strip()
            market_symbol = market_symbol.strip()
            time_data = row[1]
            
            openclose_match = re.findall(r"(\d{1,2}:\d{2} [ap]+m) - (\d{1,2}:\d{2} [ap]+m)", time_data)

            if openclose_match:

                open_local = openclose_match[0][0] 
                close_local = openclose_match[-1][1]  
                open_local = datetime.strptime(open_local.strip(), "%I:%M %p").strftime("%H:%M")
                close_local = datetime.strptime(close_local.strip(), "%I:%M %p").strftime("%H:%M")
                    
            timezone_match = re.search(r"\((.*?)\)", time_data)
            if timezone_match:
                timezone = timezone_match.group(1)

            data.append({
                'Iso2': iso2.upper(),
                'Market_Exchange': market_exchange,
                'Market_Symbol': market_symbol,
                'Open_Local': open_local,
                'Close_Local': close_local,
                'Timezone': timezone
                })
                        
        df = pd.DataFrame(data)    
               
    except Exception as e:
        print(f"Failed: {e}")
        df = pd.DataFrame()
    finally:
        driver.quit()
    
    if not df.empty:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{tradingview_path}/trading_hour_{timestamp}.json"
        df.to_json(
            filename, orient="records", indent=4, force_ascii= False
        )
        return "Scrape Trading Hour Sucessfully"
    else:
        return None



if __name__ == "__main__":
    download_trading_hour(tradingview_path = "FinanceDataScraper/database/reference_data/scraping_raw_json/tradingview")