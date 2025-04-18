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
    python3-dev \
    python3-pip \
    python3-venv \
    python3-wheel \
    python3-setuptools \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb || apt-get install -f -y \
    && rm google-chrome-stable_current_amd64.deb

RUN google-chrome --version || echo "Google Chrome installation failed"
RUN chmod +x /usr/bin/google-chrome || echo "Permission change failed for /usr/bin/google-chrome"

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install --no-cache-dir \
    selenium \
    webdriver-manager \
    ipython

CMD ["/bin/bash", "-c", "Xvfb :99 -screen 0 1920x1080x24 & export DISPLAY=:99 && python -c 'from selenium import webdriver; print(webdriver.Chrome().capabilities)'"]