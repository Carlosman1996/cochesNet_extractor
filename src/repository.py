from src.utils import ROOT_PATH
from src.utils import DatabaseOperations
from src.utils import DataframeOperations


__author__ = "Carlos Manuel Molina Sotoca"
__email__ = "cmmolinas01@gmail.com"


class Queries:
    bbdd_path = ROOT_PATH + "/bbdd/STATISTICARS.db"

    main_table_columns = [
        "ANNOUNCEMENT_ID",
        "ANNOUNCER",
        "TITLE",
        "URL",
        "PRICE",
        "WARRANTY_MONTHS",
        "IS_FINANCED",
        "IS_CERTIFIED",
        "IS_PROFESSIONAL",
        "HAS_URGE",
        "KM",
        "YEAR",
        "CC",
        "PROVINCE",
        "FUEL_TYPE",
        "PUBLISHED_DATE",
        "ENVIRONMENTAL_LABEL",
        "CREATED_DATE",
        "CREATED_USER",
        "ANNOUNCEMENT"
    ]

    statisticars_announcement_old_table_query = """
        CREATE TABLE IF NOT EXISTS ANNOUNCEMENTS_OLD (
            ID integer PRIMARY KEY AUTOINCREMENT,
            ANNOUNCEMENT_ID integer NOT NULL,
            ANNOUNCER text,
            TITLE text,
            URL text,
            PRICE integer,
            WARRANTY_MONTHS integer,
            IS_FINANCED numeric,
            IS_CERTIFIED numeric,
            IS_PROFESSIONAL numeric,
            HAS_URGE numeric,
            KM integer,
            YEAR integer,
            CC integer,
            PROVINCE text,
            FUEL_TYPE text,
            PUBLISHED_DATE numeric,
            ENVIRONMENTAL_LABEL text,
            CREATED_DATE numeric NOT NULL,
            CREATED_USER numeric NOT NULL,
            ANNOUNCEMENT blob NOT NULL
        );
    """

    statisticars_announcement_table_query = """
        CREATE TABLE IF NOT EXISTS ANNOUNCEMENT (
            ID integer PRIMARY KEY AUTOINCREMENT,
            ANNOUNCEMENT_ID integer NOT NULL,
            ANNOUNCER text,
            TITLE text,
            DESCRIPTION text,
            URL text,
            OFFER_TYPE text,
            VEHICLE_ID integer,
            VEHICLE_KM integer,
            VEHICLE_YEAR integer,
            STATUS text,
            VEHICLE_COLOR text,
            PRICE integer,
            FINANCED_PRICE integer,
            HAS_TAXES numeric,
            WARRANTY_MONTHS integer,
            WARRANTY_OFFICIAL numeric,
            IS_FINANCED numeric,
            IS_CERTIFIED numeric,
            IS_PROFESSIONAL numeric,
            HAS_URGE numeric,
            PROVINCE text,
            CREATION_DATE numeric,
            PUBLISHED_DATE numeric,
            ENVIRONMENTAL_LABEL text,
            SELLER_ID integer,
            SCRAPED_DATE numeric NOT NULL,
            SCRAPED_USER numeric NOT NULL,
            ANNOUNCEMENT blob NOT NULL
        );
    """

    statisticars_vehicle_table_query = """
        CREATE TABLE IF NOT EXISTS VEHICLE (
            ID integer PRIMARY KEY AUTOINCREMENT,
            MAKE text NOT NULL,
            MODEL text NOT NULL,
            VERSION text,
            YEAR integer,
            HORSE_POWER integer,
            FUEL_TYPE text,
            CUBIC_CAPACITY integer,
            TRANSMISSION_TYPE text,
            CO2_EMISSIONS integer,
            ENVIRONMENTAL_LABEL integer,
            DIMENSION_WIDTH integer,
            DIMENSION_HEIGHT integer,
            DIMENSION_LENGTH integer,
            WEIGHT integer,
            BODY_TYPE text,
            NUMBER_DOORS integer,
            NUMBER_SEATS integer,
            TRUNK_CAPACITY_LITERS integer,
            TANK_CAPACITY_LITERS integer,
            CONSUMPTION_URBAN real,
            CONSUMPTION_MIXED real,
            CONSUMPTION_EXTRA_URBAN real,
            MAX_SPEED integer,
            ACCELERATION integer,
            MANUFACTURER_PRICE integer
        );
    """

    read_statisticars_announcement_table_query = """
        SELECT * FROM ANNOUNCEMENTS
    """

    @staticmethod
    def create_statisticars_insert_row_query(data):
        return f"""
            INSERT INTO ANNOUNCEMENTS_OLD(ANNOUNCEMENT_ID,
                                          ANNOUNCER,
                                          TITLE,
                                          URL,
                                          PRICE,
                                          WARRANTY_MONTHS,
                                          IS_FINANCED,
                                          IS_CERTIFIED,
                                          IS_PROFESSIONAL,
                                          HAS_URGE,
                                          KM,
                                          YEAR,
                                          CC,
                                          PROVINCE,
                                          FUEL_TYPE,
                                          PUBLISHED_DATE,
                                          ENVIRONMENTAL_LABEL,
                                          CREATED_DATE,
                                          CREATED_USER,
                                          _DATA)
            VALUES(
                {data["announcement_id"]},
                {data["announcer"]},
                {data["title"]},
                {data["url"]},
                {data["price"]},
                {data["warranty_months"]},
                {data["is_financed"]},
                {data["is_certified"]},
                {data["is_professional"]},
                {data["har_urge"]},
                {data["km"]},
                {data["year"]},
                {data["cc"]},
                {data["province"]},
                {data["fuel_type"]},
                {data["published_date"]},
                {data["environmental_label"]},
                {data["created_date"]},
                {data["created_user"]},
                {data[":data"]},                
            );
        """


class Repository(Queries):
    @staticmethod
    def create_main_table():
        # Create a database connection
        conn = DatabaseOperations.create_connection(Queries.bbdd_path)

        if conn is not None:
            DatabaseOperations.execute_query(conn, Queries.statisticars_announcement_table_query)
        else:
            print("Error! Cannot create the database connection.")

    @staticmethod
    def create_vehicles_table():
        # Create a database connection
        conn = DatabaseOperations.create_connection(Queries.bbdd_path)

        if conn is not None:
            DatabaseOperations.execute_query(conn, Queries.statisticars_vehicle_table_query)
        else:
            print("Error! Cannot create the database connection.")

    @staticmethod
    def insert_row(row):
        # Create a database connection
        conn = DatabaseOperations.create_connection(Queries.bbdd_path)

        if conn is not None:
            query = Queries.create_statisticars_insert_row_query(row)
            DatabaseOperations.execute_query(conn, query)
        else:
            print("Error! Cannot create the database connection.")

    @staticmethod
    def insert_df(table, data_df):
        # Create a database connection
        conn = DatabaseOperations.create_connection(Queries.bbdd_path)

        if conn is not None:
            DataframeOperations.insert_sql(conn, table, data_df)
        else:
            print("Error! Cannot create the database connection.")

    @staticmethod
    def read_all_to_df():
        # Create a database connection
        conn = DatabaseOperations.create_connection(Queries.bbdd_path)

        if conn is not None:
            return DataframeOperations.select_sql(conn, Queries.read_statisticars_announcement_table_query)
        else:
            print("Error! Cannot create the database connection.")
            return None


if __name__ == '__main__':
    Repository.create_vehicles_table()
