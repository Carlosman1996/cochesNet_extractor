from datetime import datetime
import zoneinfo
import copy
import os
import warnings

import pandas as pd

from src.logger import Logger
from src.utils import FileOperations
from src.utils import DirectoryOperations
from src.utils import JSONFileOperations
from src.utils import ROOT_PATH
from src.cochesNet_api import CochesNetData
from src.cochesNet_api import CochesNetAPI
from src.adapters.repository import SqlAlchemyRepository as Repository
from src.cache import Cache


warnings.simplefilter(action='ignore', category=FutureWarning)

TIMEZONE_MADRID = zoneinfo.ZoneInfo("Europe/Madrid")

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


class DataExtractor:
    def __init__(self,
                 files_directory: str = ROOT_PATH + "/outputs/**/",
                 repository_obj: Repository = None,
                 logger_level="INFO"):
        self._logger_level = logger_level
        self.inputs_folder = files_directory
        # self.outputs_folder = self.inputs_folder + "/jsons/"

        # Set web to scrap:
        self._page_api = CochesNetAPI()
        self._cochesNet_data = CochesNetData()

        self._repository_obj = Repository() if repository_obj is None else repository_obj
        self._cache = Cache()

        self.vehicle_main_data = {
            "make": None,
            "model": None,
            "version": None,
            "year": None
        }

        # Set logger:
        self._logger = Logger(module=FileOperations.get_file_name(__file__, False),
                              level=self._logger_level)

    def _set_detail_files_pattern(self, search_page: str):
        self._detail_files_pattern = f"{search_page}/detail_*.json"
        return self._detail_files_pattern

    def _get_vehicle_main_data(self, detail_db_data):
        vehicle = copy.deepcopy(self.vehicle_main_data)
        vehicle["make"] = detail_db_data["MAKE"]
        vehicle["model"] = detail_db_data["MODEL"]
        vehicle["version"] = detail_db_data["VERSION"]
        vehicle["year"] = detail_db_data["YEAR"]
        return vehicle

    def process_search_data(self, search_data: dict) -> dict:
        # Read ID per announcement:
        ads_summary = []
        for number, announcement in enumerate(self._page_api.get_announcements(search_data)):
            ads_summary.append(self._page_api.get_announcement_summary(announcement))
        ads_summary_df = pd.DataFrame(ads_summary)

        # Remove duplicated data:
        ads_summary_df = ads_summary_df.drop_duplicates()

        # Check data is in cache (BBDD) or not:
        check_columns = ["title", "vehicle_year", "vehicle_km", "price"]
        merge_df = ads_summary_df.merge(self._cache.announcements_cache[check_columns],
                                        on=check_columns,
                                        how='left',
                                        indicator=True)
        ads_summary_df = merge_df[merge_df['_merge'] == 'left_only'].drop('_merge', axis=1)

        return ads_summary_df.to_dict('records')

    def _get_new_entity(self,
                       cache_df: pd.DataFrame,
                       check_columns: list,
                       data_df: pd.DataFrame) -> pd.DataFrame:
        # Remove duplicated data:
        data_processed_df = data_df.drop_duplicates()

        # Check data is in cache (BBDD) or not:
        merge_df = data_processed_df.merge(cache_df[check_columns],
                                           on=check_columns,
                                           how='left',
                                           indicator=True)
        data_processed_df = merge_df[merge_df['_merge'] == 'left_only'].drop('_merge', axis=1)
        # data_processed_df = data_processed_df.fillna(value='')
        return data_processed_df

    def get_new_sellers(self, data_df: pd.DataFrame) -> pd.DataFrame:
        data_processed_df = self._get_new_entity(cache_df=self._cache.sellers_cache,
                                                 check_columns=["name", "province"],
                                                 data_df=data_df)

        data_processed_df = data_processed_df[data_processed_df['name'].notnull()].fillna(value='')
        return data_processed_df

    def get_new_vehicles(self, data_df: pd.DataFrame) -> pd.DataFrame:
        data_processed_df = self._get_new_entity(cache_df=self._cache.vehicles_cache,
                                                 check_columns=["make", "model", "version", "year"],
                                                 data_df=data_df)

        data_processed_df = data_processed_df[data_processed_df['make'].notnull()].fillna(value='')
        data_processed_df = data_processed_df[data_processed_df['model'].notnull()].fillna(value='')
        return data_processed_df

    def get_new_announcements(self, data_df: pd.DataFrame) -> pd.DataFrame:
        data_processed_df = self._get_new_entity(cache_df=self._cache.announcements_cache,
                                                 check_columns=["title", "vehicle_year", "vehicle_km", "price", "announcer"],
                                                 data_df=data_df)
        return data_processed_df.fillna(value='')

    @staticmethod
    def _get_file_creation_date(file_path):
        c_time = os.path.getctime(file_path)
        # return str(datetime.fromtimestamp(c_time).strftime("%Y-%m-%dT%H:%M:%S:%fZ"))
        return datetime.fromtimestamp(c_time)

    def run(self):
        self._logger.set_message(level="INFO",
                                 message_level="SECTION",
                                 message="Start Data Extractor")

        # Set entities counters:
        new_announcements = 0
        new_vehicles = 0
        new_sellers = 0

        # Read pages JSONs:
        json_searchs_paths = DirectoryOperations.find_files_using_pattern(self.inputs_folder + "*.json")

        # Read search data:
        for json_search_path in json_searchs_paths:
            try:
                json_search_path = os.path.normpath(json_search_path)
                search_data_file = JSONFileOperations.read_file(json_search_path)

                # Read execution folder:
                execution_folder = json_search_path.split(os.sep)[-2]

                # Read page number:
                search_file_name = os.path.basename(json_search_path)
                search_file_split = os.path.splitext(search_file_name)
                page = search_file_split[0].replace("page_", "")

                self._logger.set_message(level="INFO",
                                         message_level="SUBSECTION",
                                         message=f"Folder {execution_folder}: Page: {page}")

                # Read details JSONs:
                search_details_folder = json_search_path.split(".")[0]
                json_details_paths = DirectoryOperations.find_files_using_pattern(self._set_detail_files_pattern(search_details_folder))

                # Read detail data:
                announcements_db_data = []
                vehicles_db_data = []
                sellers_db_data = []
                for json_detail_path in json_details_paths:
                    try:
                        # Read data:
                        detail_data_file = JSONFileOperations.read_file(json_detail_path)
                        search_detail_data = None

                        for search_data_item in search_data_file["items"]:
                            if search_data_item["id"] == detail_data_file["ad"]["id"]:
                                search_detail_data = search_data_item
                        if search_detail_data is None:
                            raise Exception(f"Search data item not found: {detail_data_file}")

                        # Read detail number:
                        detail_file_name = os.path.basename(json_detail_path)
                        detail = os.path.splitext(detail_file_name)[0].replace("detail_", "")

                        self._logger.set_message(level="DEBUG",
                                                 message_level="COMMENT",
                                                 message=f"Folder {execution_folder}:\n\tExtract data from detail: "
                                                         f"page {page} - detail {detail}")

                        # Pre-extraction data:
                        scrapped_data = {
                            "search": search_detail_data,
                            "detail": detail_data_file,
                            "scraped_date": self._get_file_creation_date(json_detail_path)
                        }

                        # Map data:
                        announcements_db_data.append(self._cochesNet_data.map_announcement_data(scrapped_data))
                        vehicles_db_data.append(self._cochesNet_data.map_vehicle_data(scrapped_data))
                        sellers_db_data.append(self._cochesNet_data.map_seller_data(scrapped_data))

                    except Exception as exception:
                        self._logger.set_message(level="ERROR",
                                                 message_level="COMMENT",
                                                 message=f"Exception in detail data processing: {str(exception)}")

                # TODO: remove this:
                sellers_db_data = pd.DataFrame(sellers_db_data)
                sellers_db_data = sellers_db_data.rename(columns=str.lower)
                vehicles_db_data = pd.DataFrame(vehicles_db_data)
                vehicles_db_data = vehicles_db_data.rename(columns=str.lower)
                announcements_db_data = pd.DataFrame(announcements_db_data)
                announcements_db_data = announcements_db_data.rename(columns=str.lower)

                # Extract only new data:
                new_sellers_db_data = self.get_new_sellers(sellers_db_data)
                new_vehicles_db_data = self.get_new_vehicles(vehicles_db_data)
                new_announcements_db_data = self.get_new_announcements(announcements_db_data)

                # Insert data:
                self._repository_obj.insert_sellers(new_sellers_db_data)
                self._repository_obj.insert_vehicles(new_vehicles_db_data)
                # self._repository_obj.insert_vehicles(announcements_db_data)
                assert 0

            except Exception as exception:
                assert 0
                self._logger.set_message(level="ERROR",
                                         message_level="COMMENT",
                                         message=f"Exception in search data processing: {str(exception)}")

        self._logger.set_message(level="INFO",
                                 message_level="COMMENT",
                                 message=f"Data extractor summary:"
                                         f"\n\tNew announcements: {new_announcements}"
                                         f"\n\tNew vehicles: {new_vehicles}"
                                         f"\n\tNew sellers: {new_sellers}")

        # TODO: Update cache


if __name__ == "__main__":
    # data_extractor = DataExtractor(files_directory=ROOT_PATH + "/outputs/1679299662/", logger_level='INFO')
    data_extractor = DataExtractor(files_directory=ROOT_PATH + "/outputs/**/", logger_level='INFO')
    data_extractor.run()
