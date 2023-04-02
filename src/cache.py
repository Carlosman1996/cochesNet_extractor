import pandas as pd
from src.logger import Logger
from src.utils import FileOperations
from src.adapters.repository import SqlAlchemyRepository as Repository


class Cache:
    def __init__(self,
                 logger_level="INFO"):
        self._logger_level = logger_level

        self._repository_obj = Repository()

        self.announcements_cache = pd.DataFrame(columns=["id",
                                                         "title",
                                                         "vehicle_year",
                                                         "vehicle_km",
                                                         "price",
                                                         "announcer"])
        self.vehicles_cache = pd.DataFrame(columns=["id",
                                                    "make",
                                                    "model",
                                                    "version",
                                                    "year"])
        self.sellers_cache = pd.DataFrame(columns=["id",
                                                   "name",
                                                   "province"])

        # Set cache:
        self.set_database_cache()

        # Set logger:
        self._logger = Logger(module=FileOperations.get_file_name(__file__, False),
                              level=self._logger_level)

    def set_database_cache(self):
        # Read announcements:
        announcements_cache = self._repository_obj.get_announcement_basic_info()
        self.announcements_cache = pd.DataFrame(announcements_cache, columns=self.announcements_cache.columns)

        # Read vehicles:
        vehicles_cache = self._repository_obj.get_vehicle_basic_info()
        self.vehicles_cache = pd.DataFrame(vehicles_cache, columns=self.vehicles_cache.columns)

        # Read sellers:
        sellers_cache = self._repository_obj.get_seller_basic_info()
        self.sellers_cache = pd.DataFrame(sellers_cache, columns=self.sellers_cache.columns)

    def update_database_cache(self,
                              announcements_data: pd.DataFrame = None,
                              vehicles_data: pd.DataFrame = None,
                              sellers_data: pd.DataFrame = None):
        # Update announcements:
        if announcements_data is not None:
            self.announcements_cache = pd.concat([self.announcements_cache, announcements_data], ignore_index=True)

        # Update vehicles:
        if vehicles_data is not None:
            self.vehicles_cache = pd.concat([self.vehicles_cache, vehicles_data], ignore_index=True)

        # Update sellers:
        if sellers_data is not None:
            self.sellers_cache = pd.concat([self.sellers_cache, sellers_data], ignore_index=True)


if __name__ == "__main__":
    cache_obj = Cache(logger_level='INFO')
    cache_obj.set_database_cache()

    print(cache_obj.announcements_cache)
    print(cache_obj.vehicles_cache)
    print(cache_obj.sellers_cache)
