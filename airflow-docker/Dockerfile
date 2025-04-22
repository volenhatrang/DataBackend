FROM apache/airflow:2.10.5

USER root

RUN apt-get update && \
    apt-get install -y wget apt-transport-https && \
    mkdir -p /etc/apt/keyrings && \
    wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | tee /etc/apt/keyrings/adoptium.asc && \
    echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list

RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz \
    && tar -xzf hadoop-3.3.6.tar.gz -C /opt/ \
    && rm hadoop-3.3.6.tar.gz \
    && ln -s /opt/hadoop-3.3.6 /opt/hadoop

# Set Hadoop environment variables
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV YARN_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin
RUN apt-get update && apt-get install -y \
    temurin-11-jdk \
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

ENV JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install --no-cache-dir \
    selenium \
    webdriver-manager \
    ipython 

CMD ["/bin/bash", "-c", "Xvfb :99 -screen 0 1920x1080x24 & export DISPLAY=:99 && python -c 'from selenium import webdriver; print(webdriver.Chrome().capabilities)'"]