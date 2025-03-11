
FROM apache/airflow:2.10.5

USER root

RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    libnss3 \
    libgconf-2-4 \
    libxss1 \
    libappindicator3-1 \
    fonts-liberation \
    libgbm-dev \
    xvfb \
    software-properties-common \
    && wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb || apt-get install -f -y \
    && rm -rf /var/lib/apt/lists/* ./google-chrome-stable_current_amd64.deb \
    && google-chrome --version || (echo "Google Chrome installation failed" && exit 1)

RUN CHROME_VERSION=$(google-chrome --version | sed 's/Google Chrome //') \
    && wget -q https://chromedriver.storage.googleapis.com/LATEST_RELEASE_${CHROME_VERSION%.*} -O /tmp/chrome_version \
    && CHROME_DRIVER_VERSION=$(cat /tmp/chrome_version) \
    && wget -q https://chromedriver.storage.googleapis.com/${CHROME_DRIVER_VERSION}/chromedriver-linux64.zip \
    && unzip chromedriver-linux64.zip -d /usr/local/bin/ \
    && rm -f chromedriver-linux64.zip /tmp/chrome_version \
    && chmod +x /usr/local/bin/chromedriver

RUN pip install --no-cache-dir \
    selenium \
    webdriver-manager

COPY scraper /app/scraper
RUN pip install --no-cache-dir /app/scraper

USER airflow

CMD ["/bin/bash", "-c", "Xvfb :99 -screen 0 1920x1080x24 & export DISPLAY=:99 && python -c 'from selenium import webdriver; print(webdriver.Chrome().capabilities)'"]