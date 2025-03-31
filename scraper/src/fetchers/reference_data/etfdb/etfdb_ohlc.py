import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))

from common_fn import *
import shutil


def download_etfdb_category_etf():
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    )

    etf_category = pd.DataFrame()
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )

    url = "https://etfdb.com/etfs/"
    driver.get(url)

    driver.implicitly_wait(30)

    etf_div = driver.find_element(By.ID, "main-table")
    divs = etf_div.find_elements(By.TAG_NAME, "div")
    div_ids = [div.get_attribute("id") for div in divs if div.get_attribute("id")]
    all_category = []

    for div_id in div_ids:
        print(div_id)
        div = driver.find_element(By.ID, div_id)
        try:
            title_element = div.find_element(By.TAG_NAME, "h3")
            title = title_element.text if title_element else "No title"
            print(title)
            show_more_button = div.find_elements(By.CLASS_NAME, "b1-tbl-toggle")

            if show_more_button:
                driver.execute_script("arguments[0].click();", show_more_button[0])
                time.sleep(2)

            table_element = div.find_element(By.CLASS_NAME, "table-no-bordered")
            table_html = (
                table_element.get_attribute("outerHTML")
                if table_element
                else "No table"
            )
            soup = BeautifulSoup(table_html, "html.parser")

            rows = soup.find_all("tr")

            headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]

            data = []
            for row in rows[1:]:
                cols = row.find_all("td")
                row_data = []

                if cols:
                    first_td = cols[0]
                    a_tag = first_td.find("a")
                    row_data.append(a_tag.get("href") if a_tag else None)
                row_data.extend([col.get_text(strip=True) for col in cols])

                data.append(row_data)

            df = pd.DataFrame(data, columns=["COMPONENT_URL"] + headers)
            df["category"] = title
            print(df)

            df = df[df["ETF THEME"] != "None"]
            all_category.append(df)
        except Exception as e:
            print(f"Failed: {e}")

    etf_category = pd.concat(all_category, ignore_index=True)
    print(etf_category)
    driver.quit()

    return etf_category


def download_etfdb_etf_components_by_url(
    pUrl="//etfdb.com/themes/artificial-intelligence-etfs/",
):
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    )

    etf_component = pd.DataFrame()
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )
    first_page_parsed = False
    all_data = []

    k = 1
    while True:
        url = f"https:{pUrl}#complete-list&sort_name=assets_under_management&sort_order=desc&page={k}"

        if not first_page_parsed:
            driver.get(url)
            driver.implicitly_wait(30)

            table_element = driver.find_element(By.CSS_SELECTOR, "#complete-list")
            nb_component = int(table_element.get_attribute("data-total-rows"))
            total_pages = int(nb_component / 25) + 1
            print(f"🔍 Number of all ETF: {nb_component}, Number pages: {total_pages}")

            first_page_parsed = True
        else:
            driver.get(url)
            driver.implicitly_wait(30)

        table_element = driver.find_element(By.CSS_SELECTOR, "#complete-list")
        table_html = (
            table_element.get_attribute("outerHTML") if table_element else "No table"
        )

        soup = BeautifulSoup(table_html, "html.parser")

        rows = soup.find_all("tr")
        headers = [
            (
                th.find(class_="full-label").get_text(strip=True)
                if th.find(class_="full-label")
                else th.get_text(strip=True)
            )
            for th in rows[0].find_all("th")
        ]

        # headers = list(
        #     dict.fromkeys([th.get_text(strip=True) for th in rows[0].find_all("th")])
        # )

        data = []
        for row in rows[1:]:
            cols = row.find_all("td")
            row_data = []
            row_data.extend([col.get_text(strip=True) for col in cols])
            data.append(row_data)

        df = pd.DataFrame(data, columns=headers)
        df = df[
            ~df.apply(
                lambda row: row.astype(str).str.contains(
                    "Click Here to Join", case=False, na=False
                )
            ).any(axis=1)
        ]
        print(df)
        print(f"📄 Page {k} scraped successfully!")

        all_data.append(df)

        if k >= total_pages:
            break
        k += 1

    etf_component = pd.concat(all_data, ignore_index=True)
    print(etf_component)
    driver.quit()
    return etf_component


def download_etfdb_prices_by_code(pCodesource="IBIT", pNbdays=30):
    # Construct URL
    purl = (
        f"https://etfflows.websol.barchart.com/proxies/timeseries/queryeod.ashx?"
        f"symbol={pCodesource}&data=daily&maxrecords={pNbdays}&volume=contract&order=asc"
        f"&dividends=false&backadjust=false&daystoexpiration=1&contractroll=expiration"
    )

    # Send request
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
        "Accept-Language": "en-US,en;q=0.5",
    }
    response = requests.get(purl, headers=headers)

    # Check response
    if response.status_code != 200:
        print("Error fetching data")
        return pd.DataFrame()

    response_text = response.text

    # Filter lines containing the desired code
    lines = [
        line for line in response_text.split("\n") if line.startswith(f"{pCodesource},")
    ]

    if not lines:
        print("No data found")
        return pd.DataFrame()

    # Convert to DataFrame
    data = pd.read_csv(io.StringIO("\n".join(lines)), header=None)
    data.columns = ["codesource", "date", "open", "high", "low", "close", "volume"]
    # Add source column
    data["source"] = "ETFDB"
    print(data)
    return data


def donwload_etfdb_shares_by_code(pCode="QQQ"):
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )

    url = f"https://etfdb.com/etf/{pCode}/#etf-ticker-profile"
    driver.get(url)
    date = driver.find_element(
        By.CSS_SELECTOR,
        "body > div.container.mm-main-container > div.row.row-no-gutters > div.col-md-4.col-sm-12.main-video-container > div.profile-container > div.row.data-timestamp > span.stock-quote-data.hidden-xs > time",
    ).get_attribute("datetime")

    element = driver.find_element(
        By.CSS_SELECTOR,
        "#overview > div:nth-child(1) > div > div:nth-child(5) > div:nth-child(1) > div > ul:nth-child(5)",
    )

    soup = BeautifulSoup(element.get_attribute("outerHTML"), "html.parser")
    rows = soup.find_all("li")

    data_dict = {}
    for row in rows:
        spans = row.find_all("span")
        if len(spans) >= 2:
            header = spans[0].text.strip()
            value = spans[1].text.strip()
            data_dict[header] = value

    df = pd.DataFrame([data_dict])
    df["ticker"] = pCode
    df["date"] = date

    print(df)
    driver.quit()
