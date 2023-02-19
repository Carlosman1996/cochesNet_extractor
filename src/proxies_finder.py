from src.proxies_page import FreeProxyListPage
from src.proxies_page import FreeProxyCzPage
import pandas as pd
import warnings
from src.postman import Postman

warnings.simplefilter(action='ignore', category=FutureWarning)


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


class ProxiesFinder:
    def __init__(self,
                 countries_filter: list = None,
                 codes_filter: list = None,
                 anonymity_filter: list = None,
                 https_filter: bool = None,
                 max_size: int = None,
                 check_proxies: bool = True):

        self._check_timeout = 1
        self._check_url = "https://www.coches.net/"
        self.freeProxyList_page = FreeProxyListPage()
        self.freeProxyCz_page = FreeProxyCzPage()

        self.proxies_df = pd.DataFrame()
        self.proxies_list = []

        # Set variables:
        self.countries_filter = countries_filter
        self.codes_filter = codes_filter
        self.anonymity_filter = anonymity_filter
        self.https_filter = https_filter
        self.max_size = max_size
        self.check_proxies = check_proxies

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

        # Get proxies:
        proxies_df = self.freeProxyList_page.get_proxies(proxies_df, self.max_size)
        # proxies_df = self.freeProxyCz_page.get_proxies(proxies_df, self.max_size)

        # Filter proxies:
        if self.countries_filter is not None and not proxies_df.empty:
            proxies_df = proxies_df[proxies_df['Country'].isin(self.countries_filter)]
        if self.codes_filter is not None and not proxies_df.empty:
            proxies_df = proxies_df[proxies_df['Code'].isin(self.codes_filter)]
        if self.anonymity_filter is not None and not proxies_df.empty:
            proxies_df = proxies_df[proxies_df['Anonymity'].isin(self.anonymity_filter)]
        if self.https_filter is not None and not proxies_df.empty:
            proxies_df = proxies_df[proxies_df['Https'] == self.https_filter]

        if not proxies_df.empty:
            # Remove unavailable proxies:
            proxies_df["available"] = \
                proxies_df["proxy"].apply(lambda proxy: self._check_proxy(proxy=proxy) if self.check_proxies else True)

            # Set proxies return variables:
            self.proxies_df = proxies_df[proxies_df["available"] == True].reset_index(drop=True)
            self.proxies_list = self.proxies_df["proxy"].tolist()

        return self.proxies_list


if __name__ == "__main__":
    px_finder_obj = ProxiesFinder(anonymity_filter=[1, 2], max_size=50)
    result = px_finder_obj.get_proxies()
    print(result)
