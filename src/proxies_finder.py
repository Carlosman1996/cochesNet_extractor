from bs4 import BeautifulSoup
import requests
from src.utils import PickleFileOperations
from src.utils import ROOT_PATH
import pandas as pd
import datetime
import zoneinfo
import warnings
import enum

warnings.simplefilter(action='ignore', category=FutureWarning)

TIMEZONE_MADRID = zoneinfo.ZoneInfo("Europe/Madrid")
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


class Anonymity(enum.Enum):
    Elite = 1
    Anonymous = 2
    Transparent = 3


class ProxiesFinder:
    def __init__(self,
                 countries_filter: list = None,
                 codes_filter: list = None,
                 anonymity_filter: int = None,
                 https_filter: bool = None,
                 max_size: int = None):

        self._base_url = "https://free-proxy-list.net/"
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
        self._proxy_model = {
            "IP_Address": str,
            "Port": str,
            "proxy": str,
            "Code": str,
            "Country": str,
            "Anonymity": bool,
            "Google": bool,
            "Https": bool,
            "Last_Checked": str,
            "created_date": datetime.__all__,
            "created_user": "ordillan",
            "available": bool
        }
        self.proxies_list = []
        self.proxies_df = pd.DataFrame(columns=list(self._proxy_model.keys()))

        # Set variables:
        self.countries_filter = countries_filter
        self.codes_filter = codes_filter
        self.anonymity_filter = anonymity_filter
        self.https_filter = https_filter
        self.max_size = max_size

    def _call_url(self):
        response = requests.get(
            url=self._base_url
        )
        if response.status_code != 200:
            raise Exception(f"URL {self._base_url} for proxy scraping is unavailable")
        return response.content

    @staticmethod
    def _type_converter(value, type):
        if type == "affirmation":
            if value == "yes":
                return True
            elif value == "no":
                return False
            else:
                return False

        if type == "Anonymity":
            if value == "elite proxy":
                return Anonymity.Elite.value
            elif value == "transparent":
                return Anonymity.Transparent.value
            elif value == "anonymous":
                return Anonymity.Anonymous.value
            else:
                return None

        raise Exception(f"Unknown type conversion: {type}")

    @staticmethod
    def _check_proxy(proxy):
        proxies = {"http": "http://" + proxy}
        try:
            requests.get("https://www.google.com/", proxies=proxies, timeout=3)
        except Exception as exception:
            print(f"Proxy: {proxy} is not available: {str(exception)}\n")
            return False
        return True

    def get_proxies(self) -> list:
        # Initialize parameters:
        proxies_df = self.proxies_df.copy()

        # Get Free Proxy HTML:
        response = self._call_url()
        html_doc = BeautifulSoup(response, 'html.parser')

        # Iterate over proxies table:
        table = html_doc.find('section', id='list').findChildren('table')[0].findChildren('tbody')[0]
        table_rows = table.findChildren('tr')

        iteration = 0
        for table_row in table_rows:
            row_cells = table_row.findChildren('td')
            proxy_model = self._proxy_model.copy()

            for index_cell, row_cell in enumerate(row_cells):
                proxy_model[self._proxy_table_indexes[index_cell]] = row_cell.text
            proxy_model["proxy"] = proxy_model["IP_Address"] + ":" + proxy_model["Port"]
            proxy_model["created_date"] = datetime.datetime.now(TIMEZONE_MADRID)
            proxy_model["Anonymity"] = self._type_converter(proxy_model["Anonymity"], type="Anonymity")
            proxy_model["Google"] = self._type_converter(proxy_model["Google"], type="affirmation")
            proxy_model["Https"] = self._type_converter(proxy_model["Https"], type="affirmation")

            # Save data on dataframe
            proxy_model_df = pd.DataFrame([proxy_model])
            proxies_df = pd.concat([proxies_df, proxy_model_df], ignore_index=True)

            # Check iteration number:
            iteration += 1
            if iteration == self.max_size:
                break

        # Filter proxies:
        if self.countries_filter is not None and not proxies_df.empty:
            proxies_df = proxies_df[proxies_df['Country'].isin(self.countries_filter)].reset_index()
        if self.codes_filter is not None and not proxies_df.empty:
            proxies_df = proxies_df[proxies_df['Code'].isin(self.codes_filter)].reset_index()
        if self.anonymity_filter is not None and not proxies_df.empty:
            proxies_df = proxies_df[proxies_df['Anonymity'] == self.anonymity_filter].reset_index()
        if self.https_filter is not None and not proxies_df.empty:
            proxies_df = proxies_df[proxies_df['Https'] == self.https_filter].reset_index()

        if not proxies_df.empty:
            # Remove unavailable proxies:
            proxies_df["available"] = proxies_df.apply(lambda proxy: self._check_proxy(proxy=proxy["proxy"]), axis=1)

            # Set proxies return variables:
            self.proxies_df = proxies_df[proxies_df["available"] == True]
            self.proxies_list = self.proxies_df["proxy"].tolist()

        return self.proxies_list


if __name__ == "__main__":
    px_finder_obj = ProxiesFinder(anonymity_filter=1, max_size=20)
    result = px_finder_obj.get_proxies()
