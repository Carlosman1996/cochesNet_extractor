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
from src.cochesNet_page import CochesNetPage
from repository import Repository

warnings.simplefilter(action='ignore', category=FutureWarning)

TIMEZONE_MADRID = zoneinfo.ZoneInfo("Europe/Madrid")

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


ROOT_PATH = str(Path(os.path.dirname(os.path.realpath(__file__))))


class DataExtractor:
    def __init__(self,
                 files_directory: str,
                 logger_level="INFO"):
        self._logger_level = logger_level
        self.inputs_folder = files_directory
        self.outputs_folder = files_directory + "/jsons/"
        self._folders_pattern = files_directory + "/**/page_*.json"

        self._cochesNet_page = CochesNetPage()

        self.data_model = dict.fromkeys(Repository.main_table_columns, None)
        self.data_df = self._set_data_df()
        self._max_data_df_len = 5000

        # Set logger:
        self._logger = Logger(module=FileOperations.get_file_name(__file__, False),
                              # logs_file_path=self.outputs_folder,
                              level=self._logger_level)

    @staticmethod
    def _set_data_df():
        return pd.DataFrame(columns=Repository.main_table_columns)

    @staticmethod
    def _get_model_value(model, keys):
        try:
            value = model
            for key in keys:
                value = value[key]
            return value
        except:
            return None

    def run(self):
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

                    # TODO: input data model is hardcoded - It changes depending on announcements source
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
