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
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb || apt-get install -f -y \
    && rm google-chrome-stable_current_amd64.deb

RUN google-chrome --version || echo "Google Chrome installation failed"

RUN chmod +x /usr/bin/google-chrome || echo "Permission change failed for /usr/bin/google-chrome"

USER airflow

RUN pip install --no-cache-dir selenium webdriver-manager apache-airflow

CMD ["google-chrome", "--version"]