from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# 設定 ChromeDriver 的選項
options = Options()
options.add_argument("--no-sandbox")
options.add_argument("--headless")  # 無頭模式，適合在伺服器環境下執行
options.add_argument("--disable-dev-shm-usage")

# 連接 Selenium Hub，而不是本地的 ChromeDriver
driver = webdriver.Remote(
    command_executor='http://selenium-hub:4444/wd/hub',  # 指定 Hub 的地址
    options=options
)

# 訪問 Google
driver.get("https://www.google.com")
print(driver.title)  # 應該會顯示 "Google"

driver.quit()
