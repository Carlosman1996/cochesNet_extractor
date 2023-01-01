import time
from datetime import datetime
import zoneinfo
import random
import os
from pathlib import Path

import pandas as pd

from src.logger import Logger
from src.utils import FileOperations
from src.utils import DirectoryOperations
from src.utils import JSONFileOperations
from src.proxies_finder import ProxiesFinder
from src.cochesNet_page import CochesNetPage


random.seed(datetime.now().timestamp())
ROOT_PATH = str(Path(os.path.dirname(os.path.realpath(__file__))))


class DataExtractor:
    def __init__(self,
                 files_directory: str,
                 logger_level="INFO"):
        self._logger_level = logger_level
        self.inputs_folder = files_directory
        self.outputs_folder = files_directory + "/jsons/"

        self._cochesNet_page = CochesNetPage()

        # Set logger:
        self._logger = Logger(module=FileOperations.get_file_name(__file__, False),
                              # logs_file_path=self.outputs_folder,
                              level=self._logger_level)

    def run(self):
        self._logger.set_message(level="INFO",
                                 message_level="SECTION",
                                 message="Start Data Extractor")

        # Read HTMLs:
        html_paths = DirectoryOperations.find_files_using_pattern(self.inputs_folder + "/page_*.html")

        # Extract cars JSON from each HTML:
        for html_path in html_paths:
            html_file = FileOperations.read_file(html_path)
            json_file = self._cochesNet_page.get_cars_dict(html_file)
            assert 0


if __name__ == "__main__":
    data_extractor = DataExtractor(files_directory=ROOT_PATH + "/outputs/1672601112", logger_level='DEBUG')
    data_extractor.run()
