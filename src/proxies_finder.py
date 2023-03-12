from src.proxies_page import FreeProxyListPage
from src.proxies_page import FreeProxyCzPage
from src.proxies_page import GeonodePage
from src.proxies_page import Common
import pandas as pd
import warnings
from src.postman import Postman
import dask.dataframe as dd
from src.logger import Logger
from src.utils import FileOperations

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
                 check_proxies: bool = True,
                 logger_level="INFO"):

        self._check_timeout = 2
        self._check_url = "https://www.coches.net/"
        self._max_number_threads = 10
        self.freeProxyList_page = FreeProxyListPage()
        self.freeProxyCz_page = FreeProxyCzPage()
        self.geonode_page = GeonodePage()
        self._logger_level = logger_level

        self.proxies_df = pd.DataFrame(columns=Common.PROXY_MODEL.keys())
        self.proxies_list = []

        # Set variables:
        self.countries_filter = countries_filter
        self.codes_filter = codes_filter
        self.anonymity_filter = anonymity_filter
        self.https_filter = https_filter
        self.max_size = max_size
        self.check_proxies = check_proxies

        # Set logger:
        self._logger = Logger(module=FileOperations.get_file_name(__file__, False),
                              # logs_file_path=self.outputs_folder,
                              level=self._logger_level)

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

    def _read_proxies_page(self, page, proxies_df):
        try:
            if page == "freeProxyList":
                proxies_df = self.freeProxyList_page.get_proxies(proxies_df)
            elif page == "geonode":
                proxies_df = self.geonode_page.get_proxies(proxies_df)
            else:
                raise Exception(f"Proxies page {page} unknown")
        except:
            self._logger.set_message(level="WARNING",
                                     message_level="MESSAGE",
                                     message=f"HTTP call to {page} page failed")
        return proxies_df

    # @Timer(text="Proxies found in {:.2f} seconds")
    def get_proxies(self) -> list:
        self._logger.set_message(level="INFO",
                                 message_level="SUBSECTION",
                                 message="Read proxies")

        # Initialize parameters:
        proxies_df = self.proxies_df.copy()

        # Get proxies:
        proxies_df = self._read_proxies_page("freeProxyList", proxies_df)
        proxies_df = self._read_proxies_page("geonode", proxies_df)

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
            # Apply max. size filter:
            if self.max_size is not None:
                proxies_df = proxies_df[:self.max_size]

            # Remove unavailable proxies:
            if self.check_proxies:
                proxies_df["available"] = dd.from_pandas(proxies_df["proxy"], npartitions=self._max_number_threads) \
                    .map_partitions(
                    lambda dframe: dframe.apply(lambda row: self._check_proxy(proxy=row) if (pd.notnull(row)) else row)).compute(
                    scheduler='threads')

            else:
                proxies_df["available"] = None

            # Set proxies return variables:
            self.proxies_df = proxies_df[proxies_df["available"] == True].reset_index(drop=True)
            self.proxies_list = self.proxies_df["proxy"].tolist()

        self._logger.set_message(level="INFO",
                                 message_level="MESSAGE",
                                 message=f"Number of available proxies: {str(len(self.proxies_list))}")

        return self.proxies_list


if __name__ == "__main__":
    px_finder_obj = ProxiesFinder(anonymity_filter=[1, 2])

    # EXEC 1:
    print("\n1 threads")
    px_finder_obj._max_number_threads = 1
    result = px_finder_obj.get_proxies()
    print(result)
    print(len(result))

    # EXEC 2:
    print("\n10 threads")
    px_finder_obj._max_number_threads = 10
    result = px_finder_obj.get_proxies()
    print(result)
    print(len(result))

    # EXEC 3:
    print("\n20 threads")
    px_finder_obj._max_number_threads = 20
    result = px_finder_obj.get_proxies()
    print(result)
    print(len(result))
