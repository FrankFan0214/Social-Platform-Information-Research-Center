# 基於官方的 Python 3.12 映像
FROM python:3.12-slim

# 安裝基本的依賴
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    wget \
    gnupg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 安裝 Chrome 瀏覽器
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux-signing-keyring.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" | tee /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 安裝 ChromeDriver
RUN CHROME_VERSION=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+') \
    && wget -O /tmp/chromedriver.zip https://chromedriver.storage.googleapis.com/$CHROME_VERSION/chromedriver_linux64.zip \
    && unzip /tmp/chromedriver.zip -d /usr/local/bin/ \
    && rm /tmp/chromedriver.zip

# 設定環境變數，讓 Chrome 正常執行（無頭模式）
ENV DISPLAY=:99

# 安裝 Selenium
RUN pip install --upgrade pip \
    && pip install selenium

# 設置工作目錄
WORKDIR /workspace/src

# 開放端口
EXPOSE 8080
