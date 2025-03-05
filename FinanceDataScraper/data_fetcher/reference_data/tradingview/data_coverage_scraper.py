from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
import json
import time
import os
from dotenv import load_dotenv


def crawler_data_coverage():
    load_dotenv(override=True)
    options = webdriver.ChromeOptions()
    # options.add_argument("--disable-gpu")
    options.add_argument("--headless")  
    # options.add_argument("--no-sandbox") 
    # options.add_argument("--disable-dev-shm-usage")  
    options.add_argument("--window-size=1920,1080")  
    # options.add_argument("--disable-software-rasterizer")
    # options.add_argument("--disable-dev-shm-usage")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    URL = "https://www.tradingview.com/data-coverage/"
    driver.get(URL) 
    tradingview_path = os.getenv("TRADINGVIEW_DATA_PATH")


    exchange_data = []
    for tab_id in ["Popular"]:
        # Start
        tab_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, f"//button[@id='{tab_id}']"))
        )
        # tab_button.click()
        driver.execute_script('arguments[0].click()', tab_button)
        time.sleep(1)

        button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, 'showLaptop-qJcpoITA')))
        if button != None:
            button.click()
        
        time.sleep(1)
        # print('html >> ', table.get_attribute("outerHTML"))

        exchange_elements = driver.find_elements(By.CSS_SELECTOR, "#tab-region-Popular tbody tr")

        for element in exchange_elements:
            exchange_name = ""
            if(len(element.find_elements(By.XPATH, ".//span[@class='exchangeName-qJcpoITA']"))):
                exchange_name = element.find_element(By.XPATH, ".//span[@class='exchangeName-qJcpoITA']").text
                if any(ex["exchangeName"] == exchange_name for ex in exchange_data):
                    continue
                
            exchange_desc_name = ""

            if(len(element.find_elements(By.XPATH, ".//span[@class='exchangeDescName-qJcpoITA']"))):
                exchange_desc_name = element.find_element(By.XPATH, ".//span[@class='exchangeDescName-qJcpoITA']").text
                pass
            
            country = ""
            if(len(element.find_elements(By.CLASS_NAME, "cell-qJcpoITA"))):
                _country_elements = element.find_elements(By.CLASS_NAME, "cell-qJcpoITA")
                _country_text = _country_elements[0].find_elements(By.TAG_NAME, "span")[-1].text
                if(_country_text not in ["CURRENCY", "SPOT", "INDICES", "SWAP", "FUTURES", "FUNDAMENTAL"]):
                    country = _country_text
                pass
            
            types = [badge.text for badge in element.find_elements(By.XPATH, ".//span[@class='content-PlSmolIm']")]
            
            exchange_data.append({
                "exchangeName": exchange_name,
                "exchangeDescName": exchange_desc_name,
                "country": country,
                "types": types,
                "tab": tab_id
            })

    # save to json ["Stocks& Indices", "Futures"]
    json_output = json.dumps(exchange_data, indent=4, ensure_ascii=False)
    with open(f"{tradingview_path}/exchanges-popular.json", "w", encoding="utf-8") as f:
        f.write(json_output)

    exchange_data = []
    for tab_id in ["Stocks& Indices", "Futures"]:
        # Start
        tab_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, f"//button[@id='{tab_id}']"))
        )
        # tab_button.click()
        driver.execute_script('arguments[0].click()', tab_button)
        time.sleep(1)

        button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, 'showLaptop-qJcpoITA')))
        if button != None:
            button.click()
        
        time.sleep(1)
        # print('html >> ', table.get_attribute("outerHTML"))

        exchange_elements = driver.find_elements(By.CLASS_NAME, "rowWrap-qJcpoITA")

        for element in exchange_elements:
            exchange_name = ""
            if(len(element.find_elements(By.XPATH, ".//span[@class='exchangeName-qJcpoITA']"))):
                exchange_name = element.find_element(By.XPATH, ".//span[@class='exchangeName-qJcpoITA']").text
                if any(ex["exchangeName"] == exchange_name for ex in exchange_data):
                    continue
                
            exchange_desc_name = ""

            if(len(element.find_elements(By.XPATH, ".//span[@class='exchangeDescName-qJcpoITA']"))):
                exchange_desc_name = element.find_element(By.XPATH, ".//span[@class='exchangeDescName-qJcpoITA']").text
                pass
            
            country = ""
            if(len(element.find_elements(By.CLASS_NAME, "cell-qJcpoITA"))):
                _country_elements = element.find_elements(By.CLASS_NAME, "cell-qJcpoITA")
                _country_text = _country_elements[0].find_elements(By.TAG_NAME, "span")[-1].text
                if(_country_text not in ["CURRENCY", "SPOT", "INDICES", "SWAP", "FUTURES", "FUNDAMENTAL"]):
                    country = _country_text
                pass
            
            types = [badge.text for badge in element.find_elements(By.XPATH, ".//span[@class='content-PlSmolIm']")]
            
            exchange_data.append({
                "exchangeName": exchange_name,
                "exchangeDescName": exchange_desc_name,
                "country": country,
                "types": types,
                "tab": tab_id
            })

    # save to json ["Stocks& Indices", "Futures"]
    json_output = json.dumps(exchange_data, indent=4, ensure_ascii=False)
    with open(f"{tradingview_path}/exchanges-stocks-indices-futures.json", "w", encoding="utf-8") as f:
        f.write(json_output)
    
    # -------------------------------------------------------------------------------------------------------- #

    exchange_data = []
    for tab_id in ["Forex"]:
        # Start
        tab_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, f"//button[@id='{tab_id}']"))
        )
        # tab_button.click()
        driver.execute_script('arguments[0].click()', tab_button)
        time.sleep(1)

        button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, 'showLaptop-qJcpoITA')))
        if button != None:
            button.click()
        
        time.sleep(1)

        exchange_elements = driver.find_elements(By.CSS_SELECTOR, "#tab-region-Forex tbody tr")

        for element in exchange_elements:
            exchange_name = ""
            if(len(element.find_elements(By.CSS_SELECTOR, ".exchangeName-qJcpoITA"))):
                exchange_name = element.find_element(By.CSS_SELECTOR, ".exchangeName-qJcpoITA").text

                if any(ex["exchangeName"] == exchange_name for ex in exchange_data):
                    continue
                
            exchange_desc_name = ""

            if(len(element.find_elements(By.XPATH, ".//span[@class='exchangeDescName-qJcpoITA']"))):
                exchange_desc_name = element.find_element(By.XPATH, ".//span[@class='exchangeDescName-qJcpoITA']").text
                pass
            
            country = ""
            if(len(element.find_elements(By.CLASS_NAME, "cell-qJcpoITA"))):
                _country_elements = element.find_elements(By.CLASS_NAME, "cell-qJcpoITA")
                _country_text = _country_elements[0].find_elements(By.TAG_NAME, "span")[-1].text
                if(_country_text not in ["CURRENCY", "SPOT", "INDICES", "SWAP", "FUTURES", "FUNDAMENTAL"]):
                    country = _country_text
                pass
            
            types = [badge.text for badge in element.find_elements(By.XPATH, ".//span[@class='content-PlSmolIm']")]
            
            exchange_data.append({
                "exchangeName": exchange_name,
                "exchangeDescName": exchange_desc_name,
                "country": country,
                "types": types,
                "tab": tab_id
            })

    # save to json ["Forex"]
    json_output = json.dumps(exchange_data, indent=4, ensure_ascii=False)
    with open(f"{tradingview_path}/exchanges-forex.json", "w", encoding="utf-8") as f:
        f.write(json_output)


    # -------------------------------------------------------------------------------------------------------- #
    exchange_data = []
    for tab_id in ["Crypto"]:
        # Start
        tab_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, f"//button[@id='{tab_id}']"))
        )
        # tab_button.click()
        driver.execute_script('arguments[0].click()', tab_button)
        time.sleep(1)

        button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, 'showLaptop-qJcpoITA')))
        if button != None:
            button.click()
        
        time.sleep(1)

        exchange_elements = driver.find_elements(By.CSS_SELECTOR, "#tab-region-Crypto tbody tr")

        for element in exchange_elements:
            exchange_name = ""
            if(len(element.find_elements(By.CSS_SELECTOR, ".exchangeName-qJcpoITA"))):
                exchange_name = element.find_element(By.CSS_SELECTOR, ".exchangeName-qJcpoITA").text

                if any(ex["exchangeName"] == exchange_name for ex in exchange_data):
                    continue
                
            exchange_desc_name = ""

            if(len(element.find_elements(By.XPATH, ".//span[@class='exchangeDescName-qJcpoITA']"))):
                exchange_desc_name = element.find_element(By.XPATH, ".//span[@class='exchangeDescName-qJcpoITA']").text
                pass
            
            country = ""
            if(len(element.find_elements(By.CLASS_NAME, "cell-qJcpoITA"))):
                _country_elements = element.find_elements(By.CLASS_NAME, "cell-qJcpoITA")
                _country_text = _country_elements[0].find_elements(By.TAG_NAME, "span")[-1].text
                if(_country_text not in ["CURRENCY", "SPOT", "INDICES", "SWAP", "FUTURES", "FUNDAMENTAL"]):
                    country = _country_text
                pass
            
            types = [badge.text for badge in element.find_elements(By.XPATH, ".//span[@class='content-PlSmolIm']")]
            
            exchange_data.append({
                "exchangeName": exchange_name,
                "exchangeDescName": exchange_desc_name,
                "country": country,
                "types": types,
                "tab": tab_id
            })

    # save to json ["Crypto"]
    json_output = json.dumps(exchange_data, indent=4, ensure_ascii=False)
    with open(f"{tradingview_path}/exchanges-crypto.json", "w", encoding="utf-8") as f:
        f.write(json_output)

    # -------------------------------------------------------------------------------------------------------- #
    exchange_data = []
    for tab_id in ["Economy"]:
        # Start
        tab_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, f"//button[@id='{tab_id}']"))
        )
        # tab_button.click()
        driver.execute_script('arguments[0].click()', tab_button)
        time.sleep(1)

        button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, 'showLaptop-qJcpoITA')))
        if button != None:
            button.click()
        
        time.sleep(1)

        exchange_elements = driver.find_elements(By.CSS_SELECTOR, "#tab-region-Economy .economyTableRow-qJcpoITA")

        for element in exchange_elements:
            exchange_name = ""
            if(len(element.find_elements(By.CSS_SELECTOR, ".exchangeName-qJcpoITA"))):
                exchange_name = element.find_element(By.CSS_SELECTOR, ".exchangeName-qJcpoITA").text

                if any(ex["exchangeName"] == exchange_name for ex in exchange_data):
                    continue
                
            country = ""
            is_world = False
            if(len(element.find_elements(By.CSS_SELECTOR, ".exchangeDescription-qJcpoITA span"))):
                country = element.find_element(By.CSS_SELECTOR, ".exchangeDescription-qJcpoITA span").text
                if(country == "World"):
                    is_world = True
                pass
            
            
            types = [badge.text for badge in element.find_elements(By.XPATH, ".//span[@class='content-PlSmolIm']")]
            
            exchange_data.append({
                "exchangeName": exchange_name,
                "exchangeDescName": exchange_desc_name,
                "country": country,
                "is_world": is_world,
                "types": types,
                "tab": tab_id
            })

    # save to json ["Economy"]
    json_output = json.dumps(exchange_data, indent=4, ensure_ascii=False)
    with open(f"{tradingview_path}/exchanges-economy.json", "w", encoding="utf-8") as f:
        f.write(json_output)
    driver.quit()

if __name__ == "__main__":
    crawler_data_coverage()
