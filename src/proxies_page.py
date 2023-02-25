import time

from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import enum
from src.postman import Postman
from src.utils import TIMEZONE_MADRID

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


class Anonymity(enum.Enum):
    ELITE = 1
    ANONYMOUS = 2
    TRANSPARENT = 3


class Common:
    PROXY_MODEL = {
        "IP_Address": str,
        "Port": str,
        "proxy": str,
        "Code": str,
        "Country": str,
        "Anonymity": bool,
        "Https": bool,
        "Last_Checked": str,
        "created_date": str,
        "created_user": "ordillan",
        "available": bool
    }

    @staticmethod
    def check_proxy_in_df(data_df, proxy):
        if proxy in data_df['proxy'].values:
            return True
        else:
            return False

    @staticmethod
    def type_converter(value, parameter_type):
        if parameter_type == "affirmation":
            if value == "yes":
                return True
            elif value == "no":
                return False
            else:
                return False

        if parameter_type == "Protocol":
            if value == "HTTPS" or value == "https":
                return True
            else:
                return False

        if parameter_type == "Anonymity":
            if value == "elite proxy" or value == "elite" or value == "Alto anonimato" or value == "Elite":
                return Anonymity.ELITE.value
            elif value == "transparent" or value == "Transparente":
                return Anonymity.TRANSPARENT.value
            elif value == "anonymous" or value == "Anónimo":
                return Anonymity.ANONYMOUS.value
            else:
                return None

        raise Exception(f"Unknown parameter type conversion: {parameter_type}")


class FreeProxyListPage(Common):
    def __init__(self):
        self.url = "https://free-proxy-list.net/"
        self._proxy_table_indexes = {
            0: "IP_Address",
            1: "Port",
            2: "Code",
            3: "Country",
            4: "Anonymity",
            5: "Google",
            6: "Https",
            7: "Last_Checked"
        }

    def get_proxies(self, proxies_df: pd.DataFrame()) -> pd.DataFrame:

        # Get Free Proxy HTML:
        response = Postman.send_request(method='GET',
                                        url=self.url,
                                        status_code_check=200)
        html_doc = BeautifulSoup(response.content, 'html.parser')

        # Iterate over proxies table:
        table = html_doc.find('section', id='list').findChildren('table')[0].findChildren('tbody')[0]
        table_rows = table.findChildren('tr')

        for table_row in table_rows:
            row_cells = table_row.findChildren('td')
            proxy_model = Common.PROXY_MODEL.copy()

            for index_cell, row_cell in enumerate(row_cells):
                proxy_model[self._proxy_table_indexes[index_cell]] = row_cell.text
            proxy_model["proxy"] = proxy_model["IP_Address"] + ":" + proxy_model["Port"]
            proxy_model["created_date"] = datetime.now(TIMEZONE_MADRID)
            proxy_model["Anonymity"] = Common.type_converter(proxy_model["Anonymity"], parameter_type="Anonymity")
            proxy_model["Https"] = Common.type_converter(proxy_model["Https"], parameter_type="affirmation")

            # Save data on dataframe
            if not Common.check_proxy_in_df(proxies_df, proxy_model["proxy"]):
                proxy_model_df = pd.DataFrame([proxy_model])
                proxies_df = pd.concat([proxies_df, proxy_model_df], ignore_index=True)
        return proxies_df


class GeonodePage(Common):
    def __init__(self):
        # TODO: apply filters in request
        self.url = "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc" \
                   "&protocols=http%2Chttps&anonymityLevel=elite&anonymityLevel=anonymous"
        self._proxy_table_indexes = {
            "ip": "IP_Address",
            "port": "Port",
            "country": "Code",
            "anonymityLevel": "Anonymity",
            "google": "Google",
            "protocols": "Https",
            "lastChecked": "Last_Checked"
        }

    def _set_url(self, page: int):
        self.url = f"https://proxylist.geonode.com/api/proxy-list?limit=500&page={page}&sort_by=lastChecked&sort_type" \
               f"=desc&protocols=http%2Chttps&anonymityLevel=elite&anonymityLevel=anonymous"

    def get_proxies(self, proxies_df: pd.DataFrame()) -> pd.DataFrame:
        page = 1
        number_proxies = 500
        while number_proxies != 0:
            # Get Free Proxy HTML:
            self._set_url(page=page)
            response = Postman.send_request(method='GET',
                                            url=self.url,
                                            status_code_check=200)
            response_data = response.json()

            # Iterate over proxies:
            number_proxies = len(response_data["data"])
            for proxy in response_data["data"]:
                proxy_model = Common.PROXY_MODEL.copy()

                for key, value in proxy.items():
                    if key in self._proxy_table_indexes.keys():
                        proxy_model[self._proxy_table_indexes[key]] = value
                proxy_model["proxy"] = proxy_model["IP_Address"] + ":" + proxy_model["Port"]
                proxy_model["created_date"] = datetime.now(TIMEZONE_MADRID)
                proxy_model["Anonymity"] = Common.type_converter(proxy_model["Anonymity"], parameter_type="Anonymity")
                proxy_model["Https"] = Common.type_converter(proxy_model["Https"][0],
                                                             parameter_type="Protocol")  # TODO: find element in list
                proxy_model["Last_Checked"] = datetime.fromtimestamp(proxy_model["Last_Checked"])

                # Save data on dataframe
                if not Common.check_proxy_in_df(proxies_df, proxy_model["proxy"]):
                    proxy_model_df = pd.DataFrame([proxy_model])
                    proxies_df = pd.concat([proxies_df, proxy_model_df], ignore_index=True)

            page += 1
        return proxies_df


# TODO:
class FreeProxyCzPage(Common):
    def __init__(self):
        self.urls = ["http://free-proxy.cz/es/proxylist/country/all/http/date/all",
                     "http://free-proxy.cz/es/proxylist/country/all/https/date/all"]
        self._proxy_table_indexes = {
            0: "IP_Address",
            1: "Port",
            2: "Protocol",
            3: "Country",
            4: "Region",
            5: "City",
            6: "Anonymity",
            7: "Speed",
            8: "Availability",
            9: "Response",
            10: "Last_Checked"
        }

    def _get_page_proxies(self, driver, proxies_df, max_size):
        # Find proxies IPs:
        self._close_popup(driver)
        driver.find_element("xpath", "//span[@id='clickexport']").click()

        element = driver.find_element("xpath", "//div[@id='zkzk']")
        proxies_ips = element.text.split('\n')

        # Iterate over proxies table:
        table_element = driver.find_element("xpath", "//table[@id='proxy_list']") \
            .find_element(By.TAG_NAME, "tbody")
        table_rows_elements = table_element.find_elements(By.TAG_NAME, "tr")

        iteration = 0
        for table_rows_element in table_rows_elements:
            row_cells_elements = table_rows_element.find_elements(By.TAG_NAME, "td")
            if len(row_cells_elements) == 11:
                proxy_model = Common.PROXY_MODEL.copy()

                for index_cell, row_cells_element in enumerate(row_cells_elements):
                    if index_cell == 0:
                        proxy_model[self._proxy_table_indexes[index_cell]] = proxies_ips[iteration]
                    else:
                        proxy_model[self._proxy_table_indexes[index_cell]] = row_cells_element.text
                proxy_model["proxy"] = proxy_model["IP_Address"] + ":" + proxy_model["Port"]
                proxy_model["created_date"] = datetime.now(TIMEZONE_MADRID)
                proxy_model["Anonymity"] = Common.type_converter(proxy_model["Anonymity"], parameter_type="Anonymity")
                proxy_model["Https"] = Common.type_converter(proxy_model["Protocol"], parameter_type="Protocol")

                # Save data on dataframe
                proxy_model_df = pd.DataFrame([proxy_model])
                proxies_df = pd.concat([proxies_df, proxy_model_df], ignore_index=True)

                # Check iteration number:
                iteration += 1
                if iteration == max_size:
                    break

    def _close_popup(self, driver):
        try:
            driver.find_element("xpath", "//*[text()='Close']").click()
        except:
            pass

    def get_proxies(self, proxies_df: pd.DataFrame(), max_size: int = None) -> dict:
        # Driver configuration:
        options = Options()
        options.headless = False
        chrome_driver_manager = ChromeDriverManager()
        driver = webdriver.Chrome(chrome_driver_manager.install(), options=options)

        # Iterate over URLs and pages and read proxies:
        for url in self.urls:
            driver.get(url)

            # Get pages:
            paginator_elements = driver.find_element("xpath", "//div[@class='paginator']"). \
                                     find_elements(By.TAG_NAME, 'a')[:-1]

            # Iterate over each page:
            for paginator_element in paginator_elements:
                proxies_df = self._get_page_proxies(driver, proxies_df, max_size)

                # Go to next page:
                self._close_popup(driver)
                paginator_element.click()

        return proxies_df
