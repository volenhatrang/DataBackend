# import investpy
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys

ticker = "VNM"
username = "0825965179"
password = "Hoainam2408."

driver_path = r"C:/chromedriver-win64/chromedriver.exe"
# download_dir = "C:/Users/TRAINEE/Downloads/"

chrome_options = Options()
# prefs = {"download.default_directory": download_dir}
# chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--force-device-scale-factor=0.9")
chrome_options.add_argument("--headless")
chrome_options.add_argument(
    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
)
service = Service(driver_path)

driver = webdriver.Chrome(service=service, options=chrome_options)
wait = WebDriverWait(driver, 10)

driver.get("https://iboard.ssi.com.vn/auth/login")

login_box = driver.find_element(
    By.CSS_SELECTOR,
    "#btnToLoginSSO",
)
login_box.click()

username_box = driver.find_element(
    By.CSS_SELECTOR,
    "#txt-username",
)
username_box.send_keys(username)

password_box = driver.find_element(
    By.CSS_SELECTOR,
    "#txt-password",
)
password_box.send_keys(password)

login_button = driver.find_element(
    By.CSS_SELECTOR,
    "#loginForm > div:nth-child(3) > button",
)
login_button.click()


driver.get("https://iboard.ssi.com.vn/analysis/fundamental-analysis")
wait.until(EC.url_contains("fundamental-analysis"))

iframes = driver.find_elements(By.TAG_NAME, "iframe")
if iframes:
    print(f"Found {len(iframes)} iframes, switching to the first one")
    driver.switch_to.frame(iframes[0])

search_ticker = wait.until(
    EC.element_to_be_clickable(
        (
            By.CSS_SELECTOR,
            "#goldenLayout > div > div > div > div.lm_items > div:nth-child(1) > div > div > div:nth-child(1) > div > div > div.d-flex.mb-10.align-items-baseline.flex-shrink-0 > div.search-box-wrapper > div.search-filter-wrapper",
        )
    )
)
search_ticker.click()
actions = ActionChains(driver)
actions.move_to_element(search_ticker).click().send_keys(ticker).send_keys(
    Keys.ENTER
).perform()

cdkt_download_button = driver.find_element(
    By.CSS_SELECTOR,
    "#goldenLayout > div > div > div > div.lm_items > div:nth-child(1) > div > div > div:nth-child(1) > div > div > div.tab-content.flex-fill.h-auto.d-flex.flex-column > div.d-flex.flex-column.flex-fill > div.d-flex.justify-content-end.flex-shrink-0 > a",
)
cdkt_download_button.click()

kqkd_tab = driver.find_element(
    By.CSS_SELECTOR,
    "#goldenLayout > div > div > div > div.lm_items > div:nth-child(1) > div > div > div:nth-child(1) > div > div > ul > li:nth-child(2) > a",
)
kqkd_tab.click()
kqkd_download_button = driver.find_element(
    By.CSS_SELECTOR,
    "#goldenLayout > div > div > div > div.lm_items > div:nth-child(1) > div > div > div:nth-child(1) > div > div > div.tab-content.flex-fill.h-auto.d-flex.flex-column > div.d-flex.flex-column.flex-fill > div.d-flex.justify-content-end.flex-shrink-0 > a",
)
kqkd_download_button.click()

lctt_tab = driver.find_element(
    By.CSS_SELECTOR,
    "#goldenLayout > div > div > div > div.lm_items > div:nth-child(1) > div > div > div:nth-child(1) > div > div > ul > li:nth-child(3) > a",
)
lctt_tab.click()
lctt_download_button = driver.find_element(
    By.CSS_SELECTOR,
    "#goldenLayout > div > div > div > div.lm_items > div:nth-child(1) > div > div > div:nth-child(1) > div > div > div.tab-content.flex-fill.h-auto.d-flex.flex-column > div.d-flex.flex-column.flex-fill > div.d-flex.justify-content-end.flex-shrink-0 > a",
)
lctt_download_button.click()

driver.switch_to.default_content()
driver.quit()
