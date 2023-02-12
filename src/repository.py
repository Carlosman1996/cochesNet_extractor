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
            CREATED_DATE numeric NOT NULL¡
            CREATED_USER numeric NOT NULL,
            ANNOUNCEMENT blob
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
            AD_CREATION_DATE numeric,
            AD_PUBLISHED_DATE numeric,
            ENVIRONMENTAL_LABEL text,
            SELLER_ID integer,
            CREATED_DATE numeric NOT NULL,
            CREATED_USER numeric NOT NULL,
            ANNOUNCEMENT blob
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
            MANUFACTURER_PRICE integer,
            CREATED_DATE numeric NOT NULL,
            CREATED_USER numeric NOT NULL
        );
    """

    statisticars_seller_table_query = """
        CREATE TABLE IF NOT EXISTS SELLER (
            ID integer PRIMARY KEY AUTOINCREMENT,
            NAME text NOT NULL,
            PAGE_URL text,
            PROVINCE text,
            ZIP_CODE text,
            CREATED_DATE numeric NOT NULL,
            CREATED_USER numeric NOT NULL
        );
    """

    read_statisticars_announcement_table_query = """
        SELECT * FROM ANNOUNCEMENTS
    """

    @staticmethod
    def create_statisticars_insert_row_query(table, data):
        values_str = ""
        for index, value in enumerate(data.values()):
            if value is None:
                values_str += "NULL"
            elif type(value) == str:
                format_value = str(value).replace("'", "''")
                values_str += f"'{format_value}'"
            else:
                values_str += f"{value}"

            if len(data.values()) > index + 1:
                values_str += ", "

        return f"""
            INSERT INTO {table} ({', '.join(data.keys())})
            VALUES ({values_str});
        """

    @staticmethod
    def create_statisticars_select_announcement_id_query(announcement_id, announcer):
        return f"""
            SELECT ID FROM ANNOUNCEMENT
            WHERE ANNOUNCEMENT_ID = {announcement_id}
            AND ANNOUNCER = '{announcer}';
        """

    @staticmethod
    def create_statisticars_select_vehicle_id_query(vehicle_make, vehicle_model, vehicle_version, vehicle_year):
        query = f"""
            SELECT ID FROM VEHICLE
            WHERE MAKE = '{vehicle_make}'
            AND MODEL = '{vehicle_model}'
        """
        if vehicle_version is not None:
            query += f"    AND VERSION = '{vehicle_version}'"
        if vehicle_year is not None:
            query += f"    AND YEAR = '{vehicle_year}'"
        query += ';'
        return query

    @staticmethod
    def create_statisticars_select_seller_id_query(seller_name, seller_province):
        query = f"""
            SELECT ID FROM SELLER
            WHERE NAME = '{seller_name}'
        """
        if seller_province is not None:
            query += f"    AND PROVINCE = '{seller_province}'"
        query += ';'
        return query


class Repository(Queries):
    @staticmethod
    def create_announcements_table():
        # Create a bbdd connection
        conn = DatabaseOperations.create_connection(Queries.bbdd_path)

        if conn is not None:
            DatabaseOperations.execute_query(conn, Queries.statisticars_announcement_table_query)
        else:
            print("Error! Cannot create the bbdd connection.")

    @staticmethod
    def create_vehicles_table():
        # Create a bbdd connection
        conn = DatabaseOperations.create_connection(Queries.bbdd_path)

        if conn is not None:
            DatabaseOperations.execute_query(conn, Queries.statisticars_vehicle_table_query)
        else:
            print("Error! Cannot create the bbdd connection.")

    @staticmethod
    def create_sellers_table():
        # Create a bbdd connection
        conn = DatabaseOperations.create_connection(Queries.bbdd_path)

        if conn is not None:
            DatabaseOperations.execute_query(conn, Queries.statisticars_seller_table_query)
        else:
            print("Error! Cannot create the bbdd connection.")

    @staticmethod
    def insert_json(table, data):
        # Create a bbdd connection
        conn = DatabaseOperations.create_connection(Queries.bbdd_path)

        if conn is not None:
            query = Queries.create_statisticars_insert_row_query(table, data)
            DatabaseOperations.insert(conn, query)
        else:
            print("Error! Cannot create the bbdd connection.")

    @staticmethod
    def insert_df(table, data_df):
        # Create a bbdd connection
        conn = DatabaseOperations.create_connection(Queries.bbdd_path)

        if conn is not None:
            DataframeOperations.insert_sql(conn, table, data_df)
        else:
            print("Error! Cannot create the bbdd connection.")

    @staticmethod
    def read_all_to_df():
        # Create a bbdd connection
        conn = DatabaseOperations.create_connection(Queries.bbdd_path)

        if conn is not None:
            return DataframeOperations.select_sql(conn, Queries.read_statisticars_announcement_table_query)
        else:
            print("Error! Cannot create the bbdd connection.")
            return None

    @staticmethod
    def get_announcement_id(announcement_id, announcer):
        # Create a bbdd connection
        conn = DatabaseOperations.create_connection(Queries.bbdd_path)

        if conn is not None:
            query = Queries.create_statisticars_select_announcement_id_query(announcement_id, announcer)
            data_ddbb = DatabaseOperations.select(conn, query)
            return data_ddbb
        else:
            print("Error! Cannot create the bbdd connection.")
            return None

    @staticmethod
    def get_vehicle_id(vehicle_make, vehicle_model, vehicle_version, vehicle_year):
        # Create a bbdd connection
        conn = DatabaseOperations.create_connection(Queries.bbdd_path)

        if conn is not None:
            query = Queries.create_statisticars_select_vehicle_id_query(vehicle_make, vehicle_model, vehicle_version, vehicle_year)
            data_ddbb = DatabaseOperations.select(conn, query)
            return data_ddbb
        else:
            print("Error! Cannot create the bbdd connection.")
            return None

    @staticmethod
    def get_seller_id(seller_name, seller_province):
        # Create a bbdd connection
        conn = DatabaseOperations.create_connection(Queries.bbdd_path)

        if conn is not None:
            query = Queries.create_statisticars_select_seller_id_query(seller_name, seller_province)
            data_ddbb = DatabaseOperations.select(conn, query)
            return data_ddbb
        else:
            print("Error! Cannot create the bbdd connection.")
            return None


if __name__ == '__main__':
    Repository.create_vehicles_table()
    Repository.create_announcements_table()
    Repository.create_sellers_table()
