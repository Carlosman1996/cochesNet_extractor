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


warnings.simplefilter(action='ignore')

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
        # Convert NaN into '':
        return data_df.fillna(value='')

    def get_complete_entity_df(self,
                               cache_df: pd.DataFrame,
                               check_columns: list,
                               data_df: pd.DataFrame) -> pd.DataFrame:
        # Check data is in cache (BBDD) or not:
        merge_df = data_df.merge(cache_df,
                                 on=check_columns,
                                 how='left',
                                 indicator=True)
        data_df["id"] = merge_df["id"]
        return data_df


"""
TODO: BUGS

2023-04-02 20:58:50,544 : INFO : data_extractor_v2 : 
 ---------- Folder 1679733385: Page: 30 ----------

2023-04-02 20:58:51,444 : INFO : data_extractor_v2 : 
 Data extractor page summary:
	New announcements: 63
	New vehicles: 7
	New sellers: 0

2023-04-02 20:58:51,450 : INFO : data_extractor_v2 : 
 ---------- Folder 1679733385: Page: 36 ----------

Error en la transacción: (pymysql.err.IntegrityError) (1048, "Column 'NAME' cannot be null")
[SQL: INSERT INTO `SELLER` (name, page_url, country, province, zip_code, created_date, created_user) VALUES (%(name)s, %(page_url)s, %(country)s, %(province)s, %(zip_code)s, %(created_date)s, %(created_user)s)]
[parameters: {'name': None, 'page_url': None, 'country': None, 'province': None, 'zip_code': None, 'created_date': Timestamp('2023-03-25 10:04:20.864163'), 'created_user': 'Ordillan'}]
(Background on this error at: http://sqlalche.me/e/gkpj)
Error en la transacción: (pymysql.err.ProgrammingError) nan can not be used with MySQL
[SQL: INSERT INTO `ANNOUNCEMENT` (announcement_id, announcer, title, description, url, offer_type, vehicle_id, vehicle_km, vehicle_year, status, vehicle_color, price, financed_price, has_taxes, warranty_months, warranty_official, is_financed, is_certified, is_professional, has_urge, country, province, ad_creation_date, ad_published_date, environmental_label, seller_id, created_date, created_user) VALUES (%(announcement_id)s, %(announcer)s, %(title)s, %(description)s, %(url)s, %(offer_type)s, %(vehicle_id)s, %(vehicle_km)s, %(vehicle_year)s, %(status)s, %(vehicle_color)s, %(price)s, %(financed_price)s, %(has_taxes)s, %(warranty_months)s, %(warranty_official)s, %(is_financed)s, %(is_certified)s, %(is_professional)s, %(has_urge)s, %(country)s, %(province)s, %(ad_creation_date)s, %(ad_published_date)s, %(environmental_label)s, %(seller_id)s, %(created_date)s, %(created_user)s)]
[parameters: {'announcement_id': '54694534', 'announcer': 'coches.net', 'title': 'VOLKSWAGEN Touran Sport 2.0 TDI 110kW 150CV 5p.', 'description': None, 'url': '/volkswagen-touran-sport-20-tdi-110kw-150cv-diesel-2019-en-valencia-54694534-covo.aspx', 'offer_type': 'Ocasión', 'vehicle_id': nan, 'vehicle_km': 90672, 'vehicle_year': 2019, 'status': 'active', 'vehicle_color': 'Blanco', 'price': 28985, 'financed_price': 26295.0, 'has_taxes': 1, 'warranty_months': 12.0, 'warranty_official': 0, 'is_financed': 1, 'is_certified': 0, 'is_professional': 1, 'has_urge': 0, 'country': None, 'province': 'Valencia', 'ad_creation_date': Timestamp('2023-03-23 20:01:34.000001'), 'ad_published_date': Timestamp('2023-03-24 19:01:46.000001'), 'environmental_label': 'C', 'seller_id': 6185.0, 'created_date': Timestamp('2023-03-25 10:04:22.300183'), 'created_user': 'Ordillan'}]
(Background on this error at: http://sqlalche.me/e/f405)
"""

    def get_entity_df_to_insert(self, data_df: pd.DataFrame, filter_null_columns: list = []):
        # Remove rows with NONEs in critical columns:
        for filter_null_column in filter_null_columns:
            data_df = data_df[data_df[filter_null_column].notnull()]

        # Remove duplicated data:
        data_df = data_df.drop_duplicates()

        # Remove rows with ID in BBDD and drop column 'id':
        data_df = data_df[data_df['id'].isnull()].drop(columns=['id'])
        return data_df

    def set_complete_entity_df(self, data_df: pd.DataFrame, ids: list):
        data_df['id'] = data_df['id'].map(lambda data_id: ids.pop(0) if data_id == '' else data_id)
        return data_df

    def insert_vehicles(self, data_list: list):
        # Convert list into dataframe:
        data_df = self.convert_list_into_df(data_list)

        # Extract only new data:
        data_df = self.get_complete_entity_df(cache_df=self._cache.vehicles_cache,
                                              check_columns=["make", "model", "version", "year"],
                                              data_df=data_df)
        new_data_df = self.get_entity_df_to_insert(data_df, filter_null_columns=['make', 'model'])

        # Insert data:
        if not new_data_df.empty:
            new_db_ids = self._repository_obj.insert_vehicles(new_data_df)

            # Set data into cache:
            data_df = self.set_complete_entity_df(data_df=data_df, ids=new_db_ids)
            self._cache.update_database_cache(vehicles_data=data_df)
        return data_df, len(new_data_df)

    def insert_sellers(self, data_list: list):
        # Convert list into dataframe:
        data_df = self.convert_list_into_df(data_list)

        # Extract only new data:
        data_df = self.get_complete_entity_df(cache_df=self._cache.sellers_cache,
                                              check_columns=["name", "province"],
                                              data_df=data_df)
        new_data_df = self.get_entity_df_to_insert(data_df, filter_null_columns=['name'])

        # Insert data:
        if not new_data_df.empty:
            new_db_ids = self._repository_obj.insert_sellers(new_data_df)

            # Set data into cache:
            data_df = self.set_complete_entity_df(data_df=data_df, ids=new_db_ids)
            self._cache.update_database_cache(sellers_data=data_df)
        return data_df, len(new_data_df)

    def insert_announcements(self, data_list: list, vehicles_ids, sellers_ids):
        # Convert list into dataframe:
        data_df = self.convert_list_into_df(data_list)

        # Extract only new data:
        data_df["vehicle_id"] = vehicles_ids
        data_df["seller_id"] = sellers_ids
        data_df = self.get_complete_entity_df(cache_df=self._cache.announcements_cache,
                                              check_columns=["title", "vehicle_year", "vehicle_km", "price", "announcer"],
                                              data_df=data_df)
        new_data_df = self.get_entity_df_to_insert(data_df, filter_null_columns=['title'])

        # Insert data:
        if not new_data_df.empty:
            new_db_ids = self._repository_obj.insert_announcements(new_data_df)

            # Set data into cache:
            data_df = self.set_complete_entity_df(data_df=data_df, ids=new_db_ids)
            self._cache.update_database_cache(announcements_data=data_df)
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

                # Insert data:
                vehicles_df_db_data, new_page_vehicles = self.insert_vehicles(vehicles_db_data)
                sellers_df_db_data, new_page_sellers = self.insert_sellers(sellers_db_data)
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
                                         message=f"Exception in search data processing: {str(exception)}")

        self._logger.set_message(level="INFO",
                                 message_level="COMMENT",
                                 message=f"Data extractor summary:"
                                         f"\n\tNew announcements: {new_announcements}"
                                         f"\n\tNew vehicles: {new_vehicles}"
                                         f"\n\tNew sellers: {new_sellers}")

        # TODO: Update cache


if __name__ == "__main__":
    # data_extractor = DataExtractor(files_directory=ROOT_PATH + "/outputs/1679733385/", logger_level='INFO')
    data_extractor = DataExtractor(files_directory=ROOT_PATH + "/outputs/**/", logger_level='INFO')
    data_extractor.run()
