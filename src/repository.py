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

    statisticars_main_table_query = """
        CREATE TABLE IF NOT EXISTS ANNOUNCEMENTS (
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

    @staticmethod
    def create_statisticars_insert_row_query(data):
        return f"""
            INSERT INTO ANNOUNCEMENTS(ANNOUNCEMENT_ID,
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
            DatabaseOperations.execute_query(conn, Queries.statisticars_main_table_query)
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


if __name__ == '__main__':
    Repository.create_main_table()
