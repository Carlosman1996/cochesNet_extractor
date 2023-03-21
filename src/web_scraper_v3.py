import threading
import queue
import time
from datetime import datetime
import zoneinfo
import random
import os

import pandas as pd

from src.logger import Logger
from src.utils import FileOperations
from src.utils import ROOT_PATH
from src.utils import JSONFileOperations
from src.proxies_finder import ProxiesFinder
from src.postman import Postman
from src.cochesNet_api import CochesNetAPI
from src.data_extractor import DataExtractor
from src.adapters.repository import SqlAlchemyRepository as Repository

TIMEZONE_MADRID = zoneinfo.ZoneInfo("Europe/Madrid")

random.seed(datetime.now().timestamp())


class WebScraper:
    def __init__(self,
                 execution_time: int = None,  # 30 minutes
                 start_page: int = None,
                 end_page: int = None,
                 find_new_proxies: bool = True,
                 logger_level="INFO"):
        self._execution_time = execution_time
        self.start_page = start_page
        self.end_page = end_page
        self._find_new_proxies = find_new_proxies
        self.proxies = []
        self._proxies_df = pd.DataFrame
        self.outputs_folder = ROOT_PATH + "/outputs/" + str(int(datetime.now().timestamp()))
        self._logger_level = logger_level
        self._proxies_wait_time = 1
        self._scrapping_wait_time = 0
        self._proxies_sleep_time = 300
        self._number_api_retries = 10
        self._max_number_threads = 10
        self._exit = False
        self._proxies_finder = False
        self._page_scrapped_details = 0

        # Set web to scrap:
        self._page_api = CochesNetAPI()

        # Data persist objects:
        self._repository_obj = Repository()

        # Set data extractor object:
        self._data_extractor_obj = DataExtractor(repository_obj=self._repository_obj,
                                                 logger_level='INFO')

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
        # TODO: implement continuous proxies reading
        # TODO: lock proxies when they are used
        if not self._proxies_finder:
            self._proxies_finder = True

            # proxies_finder = ProxiesFinder(anonymity_filter=[1, 2],
            #                                codes_filter=["US", "DE", "FR", "ES", "UK"])
            proxies_finder = ProxiesFinder(anonymity_filter=[1, 2])
            proxies_finder.get_proxies(find_new_proxies=self._find_new_proxies)
            self.proxies = proxies_finder.proxies_list
            self._proxies_df = proxies_finder.proxies_df
            self._proxies_finder = False
        else:
            while not self._proxies_finder:
                time.sleep(self._proxies_wait_time)

        number_proxies = len(self.proxies)
        if number_proxies == 0:
            self._logger.set_message(level="INFO",
                                     message_level="MESSAGE",
                                     message=f"There is any proxy available. Sleep 10 minutes-")
            time.sleep(self._proxies_sleep_time)

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

    def _get_proxy_index(self, proxy_value: str) -> int:
        return self.proxies.index(proxy_value)

    def _delete_proxies(self, proxy_indexes: list = None, proxy: str = None) -> None:
        if proxy is not None:
            proxy_indexes = [self._get_proxy_index(proxy)]
        if proxy_indexes is not None:
            self._proxies_df = self._proxies_df.drop(index=proxy_indexes).reset_index(drop=True)
            self.proxies = self._proxies_df["proxy"].tolist()

    def _get_url_content(self, request_params: dict, url_reference: int = ""):
        response_content = None
        iteration = 0
        while iteration < self._number_api_retries:
            # Review proxies availability in iteration:
            number_proxies = len(self.proxies)
            if number_proxies == 0:
                self._get_proxies()
            proxy = random.choice(self.proxies)

            try:
                time.sleep(self._scrapping_wait_time)
                response_content = self._send_request(request_params=request_params, proxy=proxy)

                if "Algo en tu navegador nos hizo pensar que eres un bot" in response_content:
                    self._logger.set_message(level="DEBUG",
                                             message_level="MESSAGE",
                                             message=f"Response HTTP Response Body: KO - forbidden: {url_reference} BOT detection"
                                                     f" - PROXY:\n{proxy}")
                    self._delete_proxies(proxy=proxy)
                    iteration += 1
                elif "You don't have permission to access /vpns/ on this server." in response_content:
                    self._logger.set_message(level="DEBUG",
                                             message_level="MESSAGE",
                                             message=f"Response HTTP Response Body: KO - forbidden: {url_reference} LOCATION detection"
                                                     f" - PROXY:\n{proxy}")
                    self._delete_proxies(proxy=proxy)
                    iteration += 1
                else:
                    self._logger.set_message(level="DEBUG",
                                             message_level="MESSAGE",
                                             message=f"Response HTTP Response Body: OK - {url_reference}"
                                                     f" - PROXY:\n{proxy}")
                    # time.sleep(30)    # TODO: freeze proxy IP during certain time - SLEEP
                    # _set_proxy_to_delete(proxy)     # TODO: freeze proxy IP during certain time - REMOVE to avoid BOT
                    break
            except Exception as exception:
                self._logger.set_message(level="DEBUG",
                                         message_level="MESSAGE",
                                         message=f"Response HTTP Response Body: KO - failed: {url_reference}\n{str(exception)}\n"
                                                 f" - PROXY:\n{proxy}")
                # Remove not valid proxies:
                self._delete_proxies(proxy=proxy)
                iteration += 1

        return response_content

    def _check_elapsed_time(self):
        if self._execution_time is not None:
            elapsed_time = self._get_elapsed_time()
            if elapsed_time > self._execution_time:
                self._logger.set_message(level="INFO",
                                         message_level="MESSAGE",
                                         message=f"Finished iterating in: {str(int(elapsed_time))} seconds: "
                                                 f"TIME ending")
                return True
        return False

    def _get_detail_data(self, index_worker: int, queue_obj: queue, current_page: int):
        while not queue_obj.empty():
            announcement = queue_obj.get()
            announcement_id = self._page_api.get_announcement_id(announcement)

            self._logger.set_message(level="DEBUG",
                                     message_level="COMMENT",
                                     message=f"Page {current_page}: read announcement detail {announcement_id}\n"
                                             f"Worker: {index_worker}, "
                                             f"PID: {os.getpid()}, "
                                             f"TID: {threading.get_ident()}")

            request_params = self._page_api.get_request_announcement(announcement=announcement)
            try:
                detail_response = self._get_url_content(request_params=request_params, url_reference=announcement_id)
                self._page_scrapped_details += 1
            except Exception as exception:
                self._logger.set_message(level="ERROR",
                                         message_level="MESSAGE",
                                         message=f"Page {current_page}: announcement detail {announcement_id} ERROR.\n"
                                                 f"{str(exception)}")
                queue_obj.put(announcement)     # Add again to queue
                detail_response = None

            if detail_response is not None:
                self._save_detail_results(page=current_page, detail=announcement_id, result=detail_response)
            queue_obj.task_done()
        return True

    def run(self):
        self._logger.set_message(level="INFO",
                                 message_level="SECTION",
                                 message="Start Web Scraper")

        # Initialize proxies:
        self._get_proxies()

        # Initialize page:
        pre_page_scrap_time = time.time()
        current_page = self.start_page if self.start_page is not None else 0

        while not self._exit:
            # Finish iterations is elapsed time is greater than maximum execution time:
            if self._exit:
                break

            # Get url page content:
            self._logger.set_message(level="INFO",
                                     message_level="SUBSECTION",
                                     message=f"Read page {current_page} content")
            pre_search_scrap_time = time.time()
            request_params = self._page_api.get_request_search_by_date_desc(page=current_page)
            search_response = self._get_url_content(request_params=request_params)

            if search_response is not None:
                # Convert and save results:
                self._save_page_results(page=current_page, result=search_response)

                # Create the queue of work
                queue_obj = queue.Queue()

                # Read ID per announcement:
                announcements = self._data_extractor_obj.process_search_data(search_response)
                [queue_obj.put(announcement) for announcement in announcements]

                # Select number workers:
                # num_available_cpus = multiprocessing.cpu_count() - 1
                num_available_cpus = self._max_number_threads
                if len(self.proxies) < num_available_cpus:
                    number_workers = len(self.proxies)
                else:
                    number_workers = num_available_cpus

                queue_size = queue_obj.qsize()
                if queue_size < number_workers:
                    number_workers = queue_size

                # Log timing: search page:
                search_scrap_time = time.time() - pre_search_scrap_time

                # Extract details data:
                self._logger.set_message(level="INFO",
                                         message_level="COMMENT",
                                         message=f"Page {current_page}: announcements to read: {queue_size}")
                pre_details_scrap_time = time.time()
                self._page_scrapped_details = 0
                for index in range(number_workers):
                    threading.Thread(target=self._get_detail_data,
                                     args=(index, queue_obj, current_page),
                                     daemon=True).start()
                queue_obj.join()

                # Log timing: detail pages:
                details_scrap_time = time.time() - pre_details_scrap_time

                # Save results on database:
                # TODO: refactor: run method can have specific outputs folder. Remove object declaration. Cache must be recalled in each rum
                data_extractor_obj = DataExtractor(files_directory=self.outputs_folder + f"/page_{str(current_page)}",
                                                   repository_obj=self._repository_obj,
                                                   logger_level='INFO')
                data_extractor_obj.run()

                # Increment page number:
                page_scrap_time = time.time() - pre_page_scrap_time
                pre_page_scrap_time = time.time()

                # Log timing stats:
                self._logger.set_message(level="INFO",
                                         message_level="COMMENT",
                                         message=f"Page {current_page}: timing statistics:"
                                                 f"\n\tTotal announcements: {self._page_scrapped_details}"
                                                 f"\n\tComplete page scrapping (seconds): {page_scrap_time}"
                                                 f"\n\tOnly page details scrapping (seconds): {details_scrap_time}"
                                                 f"\n\tOnly search page details scrapping (seconds): {search_scrap_time}")

                current_page += 1

            # Finish iterations is elapsed time is greater than maximum execution time:
            if self._check_elapsed_time():
                self._exit = True
            # Check page finish:
            if self.end_page is not None:
                if current_page > self.end_page:
                    self._logger.set_message(level="INFO",
                                             message_level="MESSAGE",
                                             message="Finished iterating: PAGE ending")
                    self._exit = True
            if search_response is not None:
                if current_page > self._page_api.get_number_pages(search_response):
                    self._exit = True

        # Save stats results:
        # TODO: DataframeOperations.save_csv(self.outputs_folder + f"/log_results_stats.csv", self._stats_df)

    def _send_request(self, request_params: dict, proxy: str):
        def _get_param(param):
            if param in request_params.keys():
                return request_params[param]
            else:
                return None

        response = Postman.send_request(
            method=_get_param("method"),
            url=_get_param("url"),
            headers=_get_param("headers"),
            json=_get_param("json"),
            http_proxy=proxy,
            timeout=20,
            status_code_check=200
        )
        return response.json()

    def _save_page_results(self, page: int, result) -> None:
        JSONFileOperations.write_file(self.outputs_folder + f"/page_{str(page)}.json",
                                      result)

    def _save_detail_results(self, page: int, detail: int, result) -> None:
        JSONFileOperations.write_file(self.outputs_folder + f"/page_{str(page)}/detail_{str(detail)}.json",
                                      result)


if __name__ == "__main__":
    # web_scraper = WebScraper(execution_time=7200, start_page=0, logger_level='DEBUG')
    web_scraper = WebScraper(start_page=0, end_page=3000, find_new_proxies=True, logger_level='INFO')
    web_scraper.run()

"""
POSSIBLE IMPROVEMENTS - SCHEMA IDEA:
1. Read search data and save information, as it's working at the moment (1 thread).
2. New web scraper method, parallel and independent, that reads search data y scrap detail if it is not present on DB (multithread):
2.0. Preconditions: method schedules and executes like searchs scraper.
2.1. Read all NEW search files in the current iteration and extract all ids.
2.2. All ids not present on BBDD are scraped.
"""
