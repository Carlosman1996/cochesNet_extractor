import os
import glob
import pickle
from pathlib import Path
import json
import yaml
import jsonschema
from jsonschema import validate
import enum
import pandas as pd
import sqlite3
from sqlite3 import Error

__author__ = "Carlos Manuel Molina Sotoca"
__email__ = "cmmolinas01@gmail.com"

ROOT_PATH = str(Path(os.path.dirname(os.path.realpath(__file__))).parent)


class PrintColors(enum.Enum):
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class Logger:
    @staticmethod
    def print_pass(string):
        print(f"{PrintColors.OKGREEN.value}{string}{PrintColors.ENDC.value}\n")

    @staticmethod
    def print_fail(string):
        print(f"{PrintColors.FAIL.value}{string}{PrintColors.ENDC.value}\n")


class DirectoryOperations:
    @staticmethod
    def check_dir_exists(dir_path):
        if os.path.isdir(dir_path):
            return True
        else:
            return False

    @staticmethod
    def create_dir(dir_path):
        try:
            os.makedirs(dir_path)
        except OSError as exception:
            print("Create directory failed: " + str(exception))

    @staticmethod
    def create_dir_by_file_path(file_path):
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
        except OSError as exception:
            print("Create directory failed: " + str(exception))

    @staticmethod
    def search_last_dir(dir_path):
        DirectoryOperations.check_dir_exists(dir_path)
        return max(glob.glob(os.path.join(dir_path, '*/')), key=os.path.getmtime)

    @staticmethod
    def find_files_using_pattern(files_pattern):
        return glob.glob(files_pattern)


class FileOperations:
    @staticmethod
    def check_file_exists(file_path):
        if os.path.isfile(file_path):
            return True
        else:
            raise Exception("File does not exist.")

    @staticmethod
    def get_file_name(file_path, extension=True):
        if extension:
            return os.path.basename(file_path)
        else:
            return os.path.basename(file_path).split('.')[0]

    @staticmethod
    def read_file(file_path):
        FileOperations.check_file_exists(file_path)
        with open(file_path, 'r', encoding="utf-8") as file_obj:
            file_content = file_obj.read()
            file_obj.close()
        return file_content

    @staticmethod
    def read_file_lines(file_path):
        FileOperations.check_file_exists(file_path)
        with open(file_path, 'r', encoding="utf-8") as file_obj:
            file_content = file_obj.readlines()
            file_obj.close()
        return file_content

    @staticmethod
    def write_file(file_path, string):
        DirectoryOperations.create_dir_by_file_path(file_path)
        with open(file_path, 'w', encoding="utf-8") as file_obj:
            file_obj.write(string)
            file_obj.close()

    @staticmethod
    def append_text_file(file_path, string):
        FileOperations.check_file_exists(file_path)
        with open(file_path, 'a', encoding="utf-8") as file_obj:
            file_obj.write(string)
            file_obj.close()


class JSONFileOperations:
    @staticmethod
    def read_file(file_path):
        FileOperations.check_file_exists(file_path)
        with open(file_path, 'r', encoding="utf-8") as json_obj:
            return json.loads(json_obj.read())

    @staticmethod
    def write_file(file_path, string):
        DirectoryOperations.create_dir_by_file_path(file_path)
        with open(file_path, 'w', encoding="utf-8") as json_obj:
            json.dump(string, json_obj, indent=4)
            json_obj.close()

    @staticmethod
    def validate_data_schema_dict(json_data, data_schema):
        try:
            validate(instance=json_data, schema=data_schema)
        except jsonschema.exceptions.ValidationError:
            return False
        return True

    @staticmethod
    def validate_data_schema_dict_of_dicts(json_data, data_schema):
        if type(json_data) is dict:
            sub_json_data = json_data.values()
            if len(sub_json_data) == 0:
                return False
            else:
                for sub_data in sub_json_data:
                    if not JSONFileOperations.validate_data_schema_dict(sub_data, data_schema):
                        return False
                return True
        else:
            return False

    @staticmethod
    def pretty_print_dict(dictionary):
        parsed = json.loads(json.dumps(dictionary, ensure_ascii=False).encode('utf8'))
        print(json.dumps(parsed, indent=4, sort_keys=True, ensure_ascii=False))


class YAMLFileOperations:
    @staticmethod
    def read_file(file_path):
        FileOperations.check_file_exists(file_path)
        with open(file_path, 'r', encoding="utf-8") as yaml_obj:
            return yaml.load(yaml_obj, Loader=yaml.FullLoader)

    @staticmethod
    def write_file(file_path, string):
        DirectoryOperations.create_dir_by_file_path(file_path)
        with open(file_path, 'w', encoding="utf-8") as yaml_obj:
            yaml.dump(string, yaml_obj, default_flow_style=False)
            yaml_obj.close()


class TextConverter:
    @staticmethod
    def snake_to_pascal_case(string):
        return string.replace('_', ' ').title().replace(' ', '')

    @staticmethod
    def snake_to_camel_case(string):
        components = string.split('_')
        return components[0] + ''.join(component.title() for component in components[1:])


class DictOperations:
    @staticmethod
    def merge_two_dicts(x, y):
        z = x.copy()  # start with keys and values of x
        z.update(y)  # modifies z with keys and values of y
        return z


class PickleFileOperations:
    @staticmethod
    def read_file(file_path):
        FileOperations.check_file_exists(file_path)
        with open(file_path, 'rb') as pickle_obj:
            return pickle.load(pickle_obj)

    @staticmethod
    def write_file(file_path, proxies):
        DirectoryOperations.create_dir_by_file_path(file_path)
        with open(file_path, 'wb') as pickle_obj:
            pickle.dump(proxies, pickle_obj)
            pickle_obj.close()


class DataframeOperations:
    @staticmethod
    def save_csv(file_path, dataframe):
        dataframe.to_csv(file_path, sep=',', encoding='utf-8', index=False)

    @staticmethod
    def save_pickle(file_path, dataframe):
        dataframe.to_pickle(file_path)

    @staticmethod
    def read_csv(file_path, converters=None):
        # TODO: Converter raises EOF error
        FileOperations.check_file_exists(file_path)
        if converters:
            df = pd.read_csv(file_path, converters=converters, sep=',', encoding='utf-8')
        else:
            df = pd.read_csv(file_path, sep=',', encoding='utf-8')
        return df

    @staticmethod
    def read_pickle(file_path):
        # TODO: Pickle is not working as expected
        pd.read_pickle(file_path)

    @staticmethod
    def convert_str_to_list(column):
        processed_column = column.copy()

        for key, item in processed_column.items():
            if type(item) == str:
                processed_column[key] = json.loads(item)
        return processed_column

    @staticmethod
    def insert_sql(conn, table_name, data_df):
        data_df.to_sql(table_name,
                       conn,
                       if_exists='append',
                       index=False)

    @staticmethod
    def select_sql(conn, query):
        return pd.read_sql(query, conn)


class DatabaseOperations:

    @staticmethod
    def create_connection(db_file):
        """ create a bbdd connection to the SQLite bbdd
            specified by db_file
        :param db_file: bbdd file
        :return: Connection object or None
        """
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            return conn
        except Error as e:
            print(e)

        return conn

    @staticmethod
    def execute_query(conn, query):
        try:
            c = conn.cursor()
            c.execute(query)
            return c
        except Error as e:
            print(e)
            print(query)
            return None

    @staticmethod
    def insert(conn, query):
        try:
            DatabaseOperations.execute_query(conn, query)
            return conn.commit()
        except Error as e:
            print(e)
            print(query)
            return None

    @staticmethod
    def select(conn, query):
        try:
            cursor = DatabaseOperations.execute_query(conn, query)
            return cursor.fetchall()
        except Error as e:
            print(e)
            print(query)
            return None
