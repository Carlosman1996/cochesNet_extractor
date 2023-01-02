import json
import time
from datetime import datetime
import zoneinfo
import random
import os
from pathlib import Path
from abc import ABC, abstractmethod

import pandas as pd

from src.logger import Logger
from src.utils import FileOperations
from src.utils import DataframeOperations
from src.utils import JSONFileOperations
from src.proxies_finder import ProxiesFinder
from src.postman import Postman
from src.cochesNet_page import CochesNetPage
from src.cochesNet_api import CochesNetAPI

TIMEZONE_MADRID = zoneinfo.ZoneInfo("Europe/Madrid")

random.seed(datetime.now().timestamp())
ROOT_PATH = str(Path(os.path.dirname(os.path.realpath(__file__))))


class WebScraperCore:
    def __init__(self,
                 execution_time: int = 3600,  # 30 minutes
                 start_page: int = None,
                 end_page: int = None,
                 logger_level="INFO"):
        self._execution_time = execution_time
        self.start_page = start_page
        self.end_page = end_page
        self.proxies = []
        self._proxies_df = pd.DataFrame
        self.outputs_folder = ROOT_PATH + "/outputs/" + str(int(datetime.now().timestamp()))
        self._logger_level = logger_level
        self._proxies_wait_time = 60

        # Timing:
        self.start_time = time.time()

        # Results logging:
        self._stats_model = {
            "page": int,
            "proxy": str,
            "country": str,
            "anonymity": bool,
            "https": bool,
            "available_proxies": int,
            "iteration_timestamp": str,
            "status_code": int,
            "result": str,
            "created_date": str,
            "created_user": "ordillan"
        }
        self._stats_df = pd.DataFrame(columns=list(self._stats_model.keys()))

        # Set logger:
        self._logger = Logger(module=FileOperations.get_file_name(__file__, False),
                              logs_file_path=self.outputs_folder,
                              level=self._logger_level)

    def _get_proxies(self):
        self._logger.set_message(level="INFO",
                                 message_level="SUBSECTION",
                                 message="Read proxies")

        proxies_finder = ProxiesFinder(anonymity_filter=[1, 2])
        proxies_finder.get_proxies()
        self.proxies = proxies_finder.proxies_list
        self._proxies_df = proxies_finder.proxies_df

        self._logger.set_message(level="INFO",
                                 message_level="MESSAGE",
                                 message=f"Number of available proxies: {str(len(self.proxies))}")

    def _delete_proxies(self, proxy_indexes: list) -> None:
        self._proxies_df = self._proxies_df.drop(index=proxy_indexes).reset_index(drop=True)
        self.proxies = self._proxies_df["proxy"].tolist()

    def _get_elapsed_time(self) -> float:
        # Check execution time:
        current_time = time.time()
        return current_time - self.start_time

    def _set_results_info(self,
                          page,
                          proxy_index,
                          status_code,
                          result):
        # TODO:
        stats_model = self._stats_model.copy()

        stats_model["page"] = page
        stats_model["proxy"] = self._proxies_df.iloc[proxy_index]["proxy"]
        stats_model["country"] = self._proxies_df.iloc[proxy_index]["Country"]
        stats_model["anonymity"] = self._proxies_df.iloc[proxy_index]["Anonymity"]
        stats_model["https"] = self._proxies_df.iloc[proxy_index]["Https"]
        stats_model["available_proxies"] = len(self.proxies)
        stats_model["iteration_timestamp"] = self._get_elapsed_time()
        stats_model["status_code"] = status_code
        stats_model["result"] = result
        stats_model["created_date"] = datetime.now(TIMEZONE_MADRID)
        stats_model["created_user"] = "ordillan"

        # Save data on dataframe
        _stats_df = pd.DataFrame([stats_model])
        self._stats_df = pd.concat([self._stats_df, _stats_df], ignore_index=True)

    def _get_page_content(self, page: int):
        self._logger.set_message(level="INFO",
                                 message_level="SUBSECTION",
                                 message=f"Read page {page} content")

        number_proxies = len(self.proxies)
        self._logger.set_message(level="INFO",
                                 message_level="MESSAGE",
                                 message=f"Available proxies: {number_proxies}")

        response_content = None
        remove_proxy_indexes = []
        def _get_proxy_index(proxy_value: str) -> int: return self.proxies.index(proxy_value)
        def _set_proxy_to_delete(proxy_value: str) -> None: remove_proxy_indexes.append(_get_proxy_index(proxy_value))

        iteration = 0
        while iteration <= number_proxies:
            # proxy = self.proxies[iteration]
            proxy = random.choice(self.proxies)
            try:
                response_content = self._send_request(page=page, proxy=proxy)

                if "Algo en tu navegador nos hizo pensar que eres un bot" in response_content:
                    self._logger.set_message(level="DEBUG",
                                             message_level="MESSAGE",
                                             message=f"Response HTTP Response Body: KO - forbidden: BOT detection"
                                                     f" - PROXY:\n{self._proxies_df.iloc[self.proxies.index(proxy)]}")
                    iteration += 1
                elif "You don't have permission to access /vpns/ on this server." in response_content:
                    self._logger.set_message(level="DEBUG",
                                             message_level="MESSAGE",
                                             message="Response HTTP Response Body: KO - forbidden: LOCATION detection"
                                                     f" - PROXY:\n{self._proxies_df.iloc[self.proxies.index(proxy)]}")
                    _set_proxy_to_delete(proxy)
                    iteration += 1
                else:
                    self._logger.set_message(level="INFO",
                                             message_level="MESSAGE",
                                             message=f"Response HTTP Response Body: OK"
                                                     f" - PROXY:\n{self._proxies_df.iloc[self.proxies.index(proxy)]}")
                    # time.sleep(30)    # TODO: freeze proxy IP during certain time - SLEEP
                    # _set_proxy_to_delete(proxy)     # TODO: freeze proxy IP during certain time - REMOVE to avoid BOT
                    break
            except Exception as exception:
                self._logger.set_message(level="DEBUG",
                                         message_level="MESSAGE",
                                         message=f"Response HTTP Response Body: KO - failed: {str(exception)}"
                                                 f" - PROXY:\n{self._proxies_df.iloc[self.proxies.index(proxy)]}")
                _set_proxy_to_delete(proxy)
                iteration += 1

        # Remove not valid proxies:
        self._delete_proxies(remove_proxy_indexes)
        return response_content

    def run(self):
        self._logger.set_message(level="INFO",
                                 message_level="SECTION",
                                 message="Start Web Scraper")

        # Initialize proxies:
        self._get_proxies()

        current_page = self.start_page if self.start_page is not None else 0

        while True:
            elapsed_time = self._get_elapsed_time()
            if elapsed_time > self._execution_time:
                self._logger.set_message(level="INFO",
                                         message_level="MESSAGE",
                                         message=f"Finished iterating in: {str(int(elapsed_time))} seconds: "
                                                 f"TIME ending")
                break

            # Check page finish:
            if self.end_page is not None:
                if current_page > self.end_page:
                    self._logger.set_message(level="INFO",
                                             message_level="MESSAGE",
                                             message=f"Finished iterating in: {str(int(elapsed_time))} seconds: "
                                                     f"PAGE ending")
                    break

            # Get url page content:
            page_response = self._get_page_content(current_page)

            if page_response is not None:
                # Convert and save results:
                self._save_results(page=current_page, result=page_response)
                current_page += 1
            else:
                time.sleep(self._proxies_wait_time)
                self._get_proxies()

        # Save stats results:
        # TODO: DataframeOperations.save_csv(self.outputs_folder + f"/log_results_stats.csv", self._stats_df)

    @abstractmethod
    def _send_request(self, page: int, proxy: str):
        return str("")

    @abstractmethod
    def _save_results(self, page: int, result: str) -> None:
        return None


class WebScraperV1(WebScraperCore):
    def __init__(self,
                 execution_time: int = 3600,
                 start_page: int = None,
                 end_page: int = None,
                 logger_level="INFO"):

        super().__init__(execution_time=execution_time,
                         start_page=start_page,
                         end_page=end_page,
                         logger_level=logger_level)

        self._cochesNet_page = CochesNetPage()

    def _send_request(self, page: int, proxy: str):
        response = Postman.send_request(
            method='POST',
            url=self._cochesNet_page.get_url_filter_most_recent(page),
            headers=self._cochesNet_page.get_headers(),
            http_proxy=proxy,
            timeout=20,
            status_code_check=200
        )
        return response.text

    def _save_results(self, page: int, result: str) -> None:
        FileOperations.write_file(self.outputs_folder + f"/page_{str(page)}.html",
                                  result)
        json_result = self._cochesNet_page.get_cars_dict_using_html(result)
        JSONFileOperations.write_file(self.outputs_folder + f"/page_{str(page)}.json",
                                      json_result)


class WebScraperV2(WebScraperCore):
    def __init__(self,
                 execution_time: int = 3600,
                 start_page: int = None,
                 end_page: int = None,
                 logger_level="INFO"):

        super().__init__(execution_time=execution_time,
                         start_page=start_page,
                         end_page=end_page,
                         logger_level=logger_level)

        self._cochesNet_api = CochesNetAPI()

    def _send_request(self, page: int, proxy: str):
        request_params = self._cochesNet_api.get_request_cars_by_relevance(page=page + 1)
        response = Postman.send_request(
            method=request_params["method"],
            url=request_params["url"],
            headers=request_params["headers"],
            json=request_params["json"],
            http_proxy=proxy,
            timeout=20,
            status_code_check=200
        )
        return json.loads(response.text)

    def _save_results(self, page: int, result: str) -> None:
        JSONFileOperations.write_file(self.outputs_folder + f"/page_{str(page)}.json",
                                      result)


if __name__ == "__main__":
    web_scraper = WebScraperV2(execution_time=3600, start_page=0, logger_level='DEBUG')
    web_scraper.run()
