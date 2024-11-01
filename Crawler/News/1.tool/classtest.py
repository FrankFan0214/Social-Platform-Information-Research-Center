# main.py
from selenium import webdriver
from utils.scroller import PageScroller

# Initialize WebDriver (e.g., Chrome) and PageScroller
driver = webdriver.Chrome()
scroller = PageScroller(driver, url="https://www.setn.com/ViewAll.aspx", max_scrolls=20, scroll_pause=1.5)

# Open the page and scroll
scroller.open_page()
scroller.scroll()