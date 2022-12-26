import os
from pathlib import Path
import time
import datetime
import re
import shutil
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.proxy import Proxy, ProxyType

from cochesNet_page import MainPage as CochesNetPage


ROOT_PATH = str(Path(os.path.dirname(os.path.realpath(__file__))))


__author__ = "Carlos Manuel Molina Sotoca"
__email__ = "cmmolinas01@gmail.com"


PROXY = "https://4BU9L424Z752UPQA0AVR57WII9KV98AS1DJMYHWMFV8R97TKAEMC2PIXGDUG5OKV1HCDCEAG915HIQPM:render_js=False&premium_proxy=True@proxy.scrapingbee.com:8886"
webdriver.DesiredCapabilities.CHROME['proxy'] = {
    "httpProxy": PROXY,
    "ftpProxy": PROXY,
    "sslProxy": PROXY,
    "noProxy": None,
    "proxyType": "MANUAL",
    "autodetect": False
}


class Search:
    def __init__(self):
        # Define time constants:
        self.search_time_sleep = 1

    def run(self):
        options = Options()
        # options.headless = True
        chrome_driver_manager = ChromeDriverManager(log_level=logging.CRITICAL, print_first_line=False)
        driver = webdriver.Chrome(chrome_driver_manager.install(), options=options)

        try:
            driver.set_window_position(0, 0)
            driver.maximize_window()

            # Open driver in the URL:
            driver.get("https://www.coches.net/segunda-mano/")
            time.sleep(self.search_time_sleep)

            # # Accept Google cookies:
            # # driver.find_element_by_xpath("//div[contains(text(), 'Acepto')]").click()
            # driver.find_element("xpath", "//button[@id='L2AGLb']").click()   # Internationalize
            # time.sleep(self.search_time_sleep)
            #
            # # Search "automatización" in Google:
            # search_text_box = driver.find_element("xpath", '//input[@class="gLFyf" and @type="text"]')
            # search_text_box.send_keys('coches net')
            # search_text_box.submit()
            # time.sleep(self.search_time_sleep)
            #
            # # Search a link to Wikipedia and click:
            # driver.find_element("xpath", "//h3[contains(text(),'Coches.net')]").click()
            # time.sleep(self.search_time_sleep)
            #
            # # Open driver in the URL:
            # time.sleep(self.search_time_sleep)

            # Read all announcements:
            self.cochesNet_page_obj = CochesNetPage(driver)
            announcements = self.cochesNet_page_obj.get_announcements()
            print(len(announcements))

        except TimeoutException as timeout_error:
            print(timeout_error)

        # Close driver
        driver.quit()


if __name__ == "__main__":
    search_obj = Search()
    search_obj.run()
