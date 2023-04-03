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
import traceback


warnings.simplefilter(action='ignore')

TIMEZONE_MADRID = zoneinfo.ZoneInfo("Europe/Madrid")

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


class DataExtractor:
    def __init__(self,
                 files_directory: str = ROOT_PATH + "/outputs/**/",
                 page: int = None,
                 repository_obj: Repository = None,
                 logger_level="INFO"):
        self._logger_level = logger_level
        self.page = page
        if self.page is None:
            self.inputs_folder = files_directory + "*.json"
        else:
            self.inputs_folder = files_directory + f"page_{self.page}.json"

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

    def convert_list_into_df(self, data_list):
        # Convert list into dataframe:
        data_df = pd.DataFrame(data_list)
        # TODO: reformat with new data mappings:
        data_df = data_df.rename(columns=str.lower)
        data_df = data_df.fillna(value='')
        return data_df

    def get_complete_entity_df(self,
                               cache_df: pd.DataFrame,
                               check_columns: list,
                               data_df: pd.DataFrame) -> pd.DataFrame:
        # Check data is in cache (BBDD) or not - remove BBDD duplicates:
        new_data_df = data_df.copy()
        merge_df = new_data_df.merge(cache_df.drop_duplicates(keep='first'),
                                 on=check_columns,
                                 how='left',
                                 indicator=False)
        new_data_df["id"] = merge_df["id"].fillna(value='')
        return new_data_df

    def get_entity_df_to_insert(self, data_df: pd.DataFrame, filter_null_columns: list = []):
        # Remove rows with NONEs in critical columns:
        for filter_null_column in filter_null_columns:
            data_df = data_df[data_df[filter_null_column] != '']

        # Remove duplicated data:
        data_df = data_df.drop_duplicates(keep='first')

        # Remove rows with ID in BBDD and drop column 'id':
        data_df = data_df[data_df['id'] == ''].drop(columns=['id'])
        return data_df

    def insert_vehicles(self, data_list: list):
        # Convert list into dataframe:
        data_df = self.convert_list_into_df(data_list)

        # Extract only new data:
        check_db_columns = ["make", "model", "version", "year"]
        new_data_df = self.get_complete_entity_df(cache_df=self._cache.vehicles_cache,
                                              check_columns=check_db_columns,
                                              data_df=data_df)
        new_data_df = self.get_entity_df_to_insert(new_data_df, filter_null_columns=['make', 'model'])

        # Insert data:
        if not new_data_df.empty:
            new_db_ids = self._repository_obj.insert_vehicles(new_data_df)
            new_data_df["id"] = new_db_ids

            # Set data into cache:
            self._cache.update_database_cache(vehicles_data=new_data_df)

            # Update complete dataframe:
            data_df = self.get_complete_entity_df(cache_df=self._cache.vehicles_cache,
                                                  check_columns=check_db_columns,
                                                  data_df=data_df)
        return data_df, len(new_data_df)

    def insert_sellers(self, data_list: list):
        # Convert list into dataframe:
        data_df = self.convert_list_into_df(data_list)

        # Extract only new data:
        check_db_columns = ["name", "province"]
        new_data_df = self.get_complete_entity_df(cache_df=self._cache.sellers_cache,
                                              check_columns=check_db_columns,
                                              data_df=data_df)
        new_data_df = self.get_entity_df_to_insert(new_data_df, filter_null_columns=['name'])

        # Insert data:
        if not new_data_df.empty:
            new_db_ids = self._repository_obj.insert_sellers(new_data_df)
            new_data_df["id"] = new_db_ids

            # Set data into cache:
            self._cache.update_database_cache(sellers_data=new_data_df)

            # Update complete dataframe:
            data_df = self.get_complete_entity_df(cache_df=self._cache.sellers_cache,
                                                  check_columns=check_db_columns,
                                                  data_df=data_df)
        return data_df, len(new_data_df)

    def insert_announcements(self, data_list: list, vehicles_ids, sellers_ids):
        # Convert list into dataframe:
        data_df = self.convert_list_into_df(data_list)

        # Extract only new data:
        data_df["vehicle_id"] = vehicles_ids
        data_df["seller_id"] = sellers_ids
        check_db_columns = ["title", "vehicle_year", "vehicle_km", "price", "announcer"]
        new_data_df = self.get_complete_entity_df(cache_df=self._cache.announcements_cache,
                                              check_columns=check_db_columns,
                                              data_df=data_df)
        new_data_df = self.get_entity_df_to_insert(new_data_df, filter_null_columns=['title'])

        # Insert data:
        if not new_data_df.empty:
            new_db_ids = self._repository_obj.insert_announcements(new_data_df)
            new_data_df["id"] = new_db_ids

            # Set data into cache:
            self._cache.update_database_cache(announcements_data=new_data_df)

            # Update complete dataframe:
            data_df = self.get_complete_entity_df(cache_df=self._cache.announcements_cache,
                                                  check_columns=check_db_columns,
                                                  data_df=data_df)
        return data_df, len(new_data_df)

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
        json_searchs_paths = DirectoryOperations.find_files_using_pattern(self.inputs_folder)

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
                save_data = True
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
                        save_data = False
                        self._logger.set_message(level="ERROR",
                                                 message_level="COMMENT",
                                                 message=f"Exception in detail data processing: {traceback.print_exc()}")

                if save_data:
                    # Insert data:
                    vehicles_df_db_data, new_page_vehicles = self.insert_vehicles(vehicles_db_data)
                    print(vehicles_df_db_data)
                    sellers_df_db_data, new_page_sellers = self.insert_sellers(sellers_db_data)
                    print(sellers_df_db_data)
                    announcements_df_db_data, new_page_announcements = \
                        self.insert_announcements(data_list=announcements_db_data,
                                                  vehicles_ids=vehicles_df_db_data['id'],
                                                  sellers_ids=sellers_df_db_data['id'])

                    # Update new data counters:
                    new_announcements += new_page_announcements
                    new_vehicles += new_page_vehicles
                    new_sellers += new_page_sellers

                self._logger.set_message(level="INFO",
                                         message_level="COMMENT",
                                         message=f"Data extractor page summary:"
                                                 f"\n\tNew announcements: {new_page_announcements}"
                                                 f"\n\tNew vehicles: {new_page_vehicles}"
                                                 f"\n\tNew sellers: {new_page_sellers}")

            except Exception as exception:
                self._logger.set_message(level="ERROR",
                                         message_level="COMMENT",
                                         message=f"Exception in search data processing: {traceback.print_exc()}")

        self._logger.set_message(level="INFO",
                                 message_level="COMMENT",
                                 message=f"Data extractor summary:"
                                         f"\n\tNew announcements: {new_announcements}"
                                         f"\n\tNew vehicles: {new_vehicles}"
                                         f"\n\tNew sellers: {new_sellers}")

        # TODO: Update cache


if __name__ == "__main__":
    # data_extractor = DataExtractor(files_directory=ROOT_PATH + "/outputs/1679733385/", page=18, logger_level='INFO')
    data_extractor = DataExtractor(files_directory=ROOT_PATH + "/outputs/**/", logger_level='INFO')
    data_extractor.run()
