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


ROOT_PATH = str(Path(os.path.dirname(os.path.realpath(__file__))))


__author__ = "Carlos Manuel Molina Sotoca"
__email__ = "carlosmanuel.molinasotoca@everis.nttdata.com"


class Search:
    def __init__(self):
        # Define time constants:
        self.search_time_sleep = 1

    @staticmethod
    def create_outputs_directory(outputs_path=None):
        # Create outputs path:
        folder_name_obj = datetime.datetime.now()
        test_name = str(folder_name_obj).replace(" ", "__").replace(":", "_").replace(".", "_")
        if outputs_path is None:
            outputs_path = ROOT_PATH + "\\outputs\\" + test_name

        # Remove outputs directory if it exists:
        if os.path.isdir(outputs_path):
            shutil.rmtree(outputs_path)

        # Create outputs directory:
        os.makedirs(outputs_path)
        return outputs_path

    @staticmethod
    def year_check_conditions(paragraph_text="", find_year=None):
        year_result = None

        # Read all numbers paragraph:
        regex_result = re.findall('[0-9]+', paragraph_text)
        numbers = [int(number) for number in regex_result]
        if find_year is not None:
            if find_year in numbers:
                year_result = find_year
        else:
            # Iterate over each number:
            year_results = []
            if "automático" in paragraph_text:
                for number in numbers:
                    if 2000 > number > 1500:
                        year_results.append(number)

            # Choose lower number:
            if len(year_results) > 0:
                year_result = min(year_results)
        return year_result

    @staticmethod
    def capture_screenshot(driver, page_element, outputs_path, style="4px solid red"):
        _ = page_element.location_once_scrolled_into_view

        # Get paragraph border
        original_style = driver.execute_script("return arguments[0].style.border;", page_element)

        # Set new border:
        driver.execute_script("arguments[0].style.border = '" + style + "';", page_element)

        # Save image:
        driver.save_screenshot(outputs_path + "//year_paragraph.png")

        # Reset border:
        driver.execute_script("arguments[0].style.border = '" + original_style + "';", page_element)

    def run(self, find_year=None, outputs_path=None):
        year_result = None
        outputs_path = self.create_outputs_directory(outputs_path)

        # Driver configuration:
        options = Options()
        # options.headless = True
        chrome_driver_manager = ChromeDriverManager(log_level=logging.CRITICAL, print_first_line=False)
        driver = webdriver.Chrome(chrome_driver_manager.install(), options=options)

        try:
            driver.set_window_position(0, 0)
            driver.maximize_window()

            # Open driver in the URL:
            driver.get("https://www.google.es")
            time.sleep(self.search_time_sleep)

            # Accept Google cookies:
            # driver.find_element_by_xpath("//div[contains(text(), 'Acepto')]").click()
            driver.find_element_by_xpath("//button[@id='L2AGLb']").click()   # Internationalize
            time.sleep(self.search_time_sleep)

            # Search "automatización" in Google:
            search_text_box = driver.find_element_by_xpath('//input[@class="gLFyf gsfi" and @type="text"]')
            search_text_box.send_keys('automatización')
            search_text_box.submit()
            time.sleep(self.search_time_sleep)

            # Search a link to Wikipedia and click:
            driver.find_element_by_xpath("//h3[contains(text(),'Automatización industrial - "
                                         "Wikipedia, la enciclop')]").click()
            time.sleep(self.search_time_sleep)

            # SEARCH FIRST DATE: The paragraph must contain a year and the word "automático":
            paragraph_elements = driver.find_elements_by_xpath("//div[@id='mw-content-text']//p")
            time.sleep(self.search_time_sleep)

            # Iterate over each paragraph:
            for paragraph_element in paragraph_elements:
                # Get paragraph text:
                paragraph_text = paragraph_element.text

                # Check number in paragraph text:
                year_result = self.year_check_conditions(paragraph_text=paragraph_text, find_year=find_year)

                # Stop iterating if a year with the requisites has been found:
                if year_result is not None:
                    # Save screenshot:
                    self.capture_screenshot(driver=driver, page_element=paragraph_element, outputs_path=outputs_path)
                    break

        except TimeoutException as timeout_error:
            print(timeout_error)

        # Close driver
        driver.quit()
        return year_result


if __name__ == "__main__":
    search_obj = Search()
    result = search_obj.run(outputs_path=".//outputs")
    print(result)
