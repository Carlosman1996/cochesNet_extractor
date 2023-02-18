from datetime import datetime
import zoneinfo
import copy
import os
import warnings
from pathlib import Path

import pandas as pd

from src.logger import Logger
from src.utils import FileOperations
from src.utils import DirectoryOperations
from src.utils import JSONFileOperations
from src.utils import ROOT_PATH
from src.cochesNet_api import CochesNetData
from src.repository import Repository

warnings.simplefilter(action='ignore', category=FutureWarning)

TIMEZONE_MADRID = zoneinfo.ZoneInfo("Europe/Madrid")

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


class DataExtractor:
    def __init__(self,
                 files_directory: str,
                 logger_level="INFO"):
        self._logger_level = logger_level
        self.inputs_folder = files_directory
        self.outputs_folder = self.inputs_folder + "/jsons/"
        self._search_files_pattern = self.inputs_folder + "/**/page_*.json"
        self._detail_files_pattern = None

        self._cochesNet_data = CochesNetData()

        self.data_model = dict.fromkeys(Repository.main_table_columns, None)
        self.data_df = self._set_data_df()
        self._max_data_df_len = 5000

        self.vehicle_main_data = {
            "make": None,
            "model": None,
            "version": None,
            "year": None
        }

        # Set logger:
        self._logger = Logger(module=FileOperations.get_file_name(__file__, False),
                              # logs_file_path=self.outputs_folder,
                              level=self._logger_level)

    def _set_detail_files_pattern(self, search_page: str):
        self._detail_files_pattern = f"{search_page}/detail_*.json"
        return self._detail_files_pattern

    @staticmethod
    def _set_data_df():
        return pd.DataFrame(columns=Repository.main_table_columns)

    def _get_vehicle_main_data(self, detail_db_data):
        vehicle = copy.deepcopy(self.vehicle_main_data)
        vehicle["make"] = detail_db_data["MAKE"]
        vehicle["model"] = detail_db_data["MODEL"]
        vehicle["version"] = detail_db_data["VERSION"]
        vehicle["year"] = detail_db_data["YEAR"]
        return vehicle

    @staticmethod
    def _get_file_creation_date(file_path):
        c_time = os.path.getctime(file_path)
        return str(datetime.fromtimestamp(c_time).strftime("%Y-%m-%dT%H:%M:%S:%fZ"))

    def run(self):
        self._logger.set_message(level="INFO",
                                 message_level="SECTION",
                                 message="Start Data Extractor")

        # Set entities counters:
        new_announcements = 0
        new_vehicles = 0
        new_sellers = 0

        # Read pages JSONs:
        json_searchs_paths = DirectoryOperations.find_files_using_pattern(self._search_files_pattern)

        # Read search data:
        for json_search_path in json_searchs_paths:
            try:
                search_data_file = JSONFileOperations.read_file(json_search_path)

                # Read page number:
                search_file_name = os.path.basename(json_search_path)
                page = os.path.splitext(search_file_name)[0].replace("page_", "")

                self._logger.set_message(level="INFO",
                                         message_level="SUBSECTION",
                                         message=f"Extract data from page: {page}")

                # Read details JSONs:
                search_details_folder = json_search_path.split(".")[0]
                json_details_paths = DirectoryOperations.find_files_using_pattern(self._set_detail_files_pattern(search_details_folder))

                # Read detail data:
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
                                                 message=f"Extract data from detail: page {page} - detail {detail}")

                        # Pre-extraction data:
                        scrapped_data = {
                            "search": search_detail_data,
                            "detail": detail_data_file,
                            "scraped_date": self._get_file_creation_date(json_detail_path)
                        }

                        # Extract data:
                        announcement_db_data = self._cochesNet_data.map_announcement_data(scrapped_data)
                        vehicle_db_data = self._cochesNet_data.map_vehicle_data(scrapped_data)
                        seller_db_data = self._cochesNet_data.map_seller_data(scrapped_data)

                        # Check data already exists:
                        equal_announcements = Repository.get_announcement_id(announcement_db_data["ANNOUNCEMENT_ID"],
                                                                             announcement_db_data["ANNOUNCER"])
                        if len(equal_announcements) == 0 or equal_announcements is None:
                            # Insert vehicle:
                            vehicle = Repository.get_vehicle_id(vehicle_db_data["MAKE"],
                                                                vehicle_db_data["MODEL"],
                                                                vehicle_db_data["VERSION"],
                                                                vehicle_db_data["YEAR"])
                            if len(vehicle) == 0 or vehicle is None:
                                Repository.insert_json("VEHICLE", vehicle_db_data)
                                new_vehicles += 1
                                vehicle = Repository.get_vehicle_id(vehicle_db_data["MAKE"],
                                                                    vehicle_db_data["MODEL"],
                                                                    vehicle_db_data["VERSION"],
                                                                    vehicle_db_data["YEAR"])
                            announcement_db_data["VEHICLE_ID"] = vehicle[0][0]

                            # Insert seller:
                            seller = Repository.get_seller_id(seller_db_data["NAME"], seller_db_data["PROVINCE"])
                            if seller_db_data["NAME"] is not None:
                                if len(seller) == 0 or seller is None:
                                    Repository.insert_json("SELLER", seller_db_data)
                                    new_sellers += 1
                                    seller = Repository.get_seller_id(seller_db_data["NAME"],
                                                                      seller_db_data["PROVINCE"])
                                announcement_db_data["SELLER_ID"] = seller[0][0]
                            else:
                                announcement_db_data["SELLER_ID"] = None

                            # Insert announcement:
                            Repository.insert_json("ANNOUNCEMENT", announcement_db_data)
                            new_announcements += 1
                    except Exception as exception:
                        self._logger.set_message(level="DEBUG",
                                                 message_level="COMMENT",
                                                 message=f"Exception in detail data processing: {str(exception)}")
            except Exception as exception:
                self._logger.set_message(level="DEBUG",
                                         message_level="COMMENT",
                                         message=f"Exception in search data processing: {str(exception)}")

        self._logger.set_message(level="INFO",
                                 message_level="COMMENT",
                                 message=f"Data extractor summary:"
                                         f"\n\tNew announcements: {new_announcements}"
                                         f"\n\tNew vehicles: {new_vehicles}"
                                         f"\n\tNew sellers: {new_sellers}")

    def old_run(self):
        number_rows_read = 0

        self._logger.set_message(level="INFO",
                                 message_level="SECTION",
                                 message="Start Data Extractor")

        # Read HTMLs:
        json_paths = DirectoryOperations.find_files_using_pattern(self._folders_pattern)

        # Insert data:
        for json_path in json_paths:
            data_file = JSONFileOperations.read_file(json_path)

            self._logger.set_message(level="DEBUG",
                                     message_level="SUBSECTION",
                                     message=f"Extract data from: {json_path}")

            if "items" in data_file.keys():
                number_rows_file = len(data_file['items'])
                self._logger.set_message(level="DEBUG",
                                         message_level="COMMENT",
                                         message=f"Number rows in file: {number_rows_file}")
                number_rows_read += number_rows_file

                # Extract data:
                for data_item in data_file["items"]:
                    data_model = copy.deepcopy(self.data_model)

                    data_model["ANNOUNCEMENT_ID"] = data_item["id"]
                    data_model["ANNOUNCER"] = "Coches.net"
                    data_model["TITLE"] = self._get_model_value(data_item, ["title"])
                    data_model["URL"] = self._get_model_value(data_item, ["url"])
                    data_model["PRICE"] = self._get_model_value(data_item, ["price", "amount"])
                    data_model["WARRANTY_MONTHS"] = self._get_model_value(data_item, ["warranty", "months"])
                    data_model["IS_FINANCED"] = self._get_model_value(data_item, ["isFinanced"])
                    data_model["IS_CERTIFIED"] = self._get_model_value(data_item, ["isCertified"])
                    data_model["IS_PROFESSIONAL"] = self._get_model_value(data_item, ["isProfessional"])
                    data_model["HAS_URGE"] = self._get_model_value(data_item, ["hasUrge"])
                    data_model["KM"] = self._get_model_value(data_item, ["km"])
                    data_model["YEAR"] = self._get_model_value(data_item, ["year"])
                    data_model["CC"] = self._get_model_value(data_item, ["cubicCapacity"])
                    data_model["PROVINCE"] = self._get_model_value(data_item, ["mainProvince"])
                    data_model["FUEL_TYPE"] = self._get_model_value(data_item, ["fuelType"])
                    data_model["PUBLISHED_DATE"] = self._get_model_value(data_item, ["publishedDate"])
                    data_model["ENVIRONMENTAL_LABEL"] = self._get_model_value(data_item, ["environmentalLabel"])
                    data_model["CREATED_DATE"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                    data_model["CREATED_USER"] = "Ordillan"
                    data_model["ANNOUNCEMENT"] = str(data_item)

                    # Save data on dataframe
                    data_model_df = pd.DataFrame([data_model])
                    self.data_df = pd.concat([self.data_df, data_model_df], ignore_index=True)

                self._logger.set_message(level="DEBUG",
                                         message_level="COMMENT",
                                         message=f"Number rows read: {number_rows_read}")

            # Check dataframe length - insert data and reset:
            if len(self.data_df) > self._max_data_df_len:
                self._logger.set_message(level="INFO",
                                         message_level="COMMENT",
                                         message=f"Saving data on BBDD: {len(self.data_df)} new rows")
                Repository.insert_df("ANNOUNCEMENTS", self.data_df)

                self.data_df = self._set_data_df()


if __name__ == "__main__":
    # data_extractor = DataExtractor(files_directory=ROOT_PATH + "/outputs/1672601112", logger_level='DEBUG')
    data_extractor = DataExtractor(files_directory=ROOT_PATH + "/outputs/", logger_level='INFO')
    data_extractor.run()
