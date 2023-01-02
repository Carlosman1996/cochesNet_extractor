from bs4 import BeautifulSoup
from src.utils import PickleFileOperations
from src.utils import ROOT_PATH
import pandas as pd
import datetime
import zoneinfo
import warnings
import enum
from src.postman import Postman
from src.cochesNet_page import CochesNetPage

warnings.simplefilter(action='ignore', category=FutureWarning)

TIMEZONE_MADRID = zoneinfo.ZoneInfo("Europe/Madrid")

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


class Anonymity(enum.Enum):
    ELITE = 1
    ANONYMOUS = 2
    TRANSPARENT = 3


class ProxiesFinder:
    def __init__(self,
                 countries_filter: list = None,
                 codes_filter: list = None,
                 anonymity_filter: list = None,
                 https_filter: bool = None,
                 google_filter: bool = None,
                 max_size: int = None,
                 check_proxies: bool = True):

        self._base_url = "https://free-proxy-list.net/"
        self._check_timeout = 3
        self._check_url = "https://www.coches.net/"
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
        self.proxies_df = pd.DataFrame(columns=list(self._proxy_model.keys()))
        self.proxies_list = []

        # Set variables:
        self.countries_filter = countries_filter
        self.codes_filter = codes_filter
        self.anonymity_filter = anonymity_filter
        self.https_filter = https_filter
        self.google_filter = google_filter
        self.max_size = max_size
        self.check_proxies = check_proxies

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
                return Anonymity.ELITE.value
            elif value == "transparent":
                return Anonymity.TRANSPARENT.value
            elif value == "anonymous":
                return Anonymity.ANONYMOUS.value
            else:
                return None

        raise Exception(f"Unknown type conversion: {type}")

    def _check_proxy(self, proxy):
        try:
            Postman.send_request(method="GET",
                                 url=self._check_url,
                                 http_proxy=proxy,
                                 timeout=self._check_timeout)
        except Exception as exception:
            # print(f"Proxy: {proxy} is not available: {str(exception)}\n")
            return False
        return True

    def get_proxies(self) -> list:
        # Initialize parameters:
        proxies_df = self.proxies_df.copy()

        # Get Free Proxy HTML:
        response = Postman.send_request(method='GET',
                                        url=self._base_url,
                                        status_code_check=200)
        html_doc = BeautifulSoup(response.content, 'html.parser')

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
            proxies_df = proxies_df[proxies_df['Country'].isin(self.countries_filter)]
        if self.codes_filter is not None and not proxies_df.empty:
            proxies_df = proxies_df[proxies_df['Code'].isin(self.codes_filter)]
        if self.anonymity_filter is not None and not proxies_df.empty:
            proxies_df = proxies_df[proxies_df['Anonymity'].isin(self.anonymity_filter)]
        if self.https_filter is not None and not proxies_df.empty:
            proxies_df = proxies_df[proxies_df['Https'] == self.https_filter]
        if self.google_filter is not None and not proxies_df.empty:
            proxies_df = proxies_df[proxies_df['Google'] == self.google_filter]

        if not proxies_df.empty:
            # Remove unavailable proxies:
            proxies_df["available"] = \
                proxies_df["proxy"].apply(lambda proxy: self._check_proxy(proxy=proxy) if self.check_proxies else True)

            # Set proxies return variables:
            self.proxies_df = proxies_df[proxies_df["available"] == True].reset_index(drop=True)
            self.proxies_list = self.proxies_df["proxy"].tolist()

        return self.proxies_list


if __name__ == "__main__":
    px_finder_obj = ProxiesFinder(anonymity_filter=[1, 2], google_filter=True, max_size=50)
    result = px_finder_obj.get_proxies()
    print(result)
