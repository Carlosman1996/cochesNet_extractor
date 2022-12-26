import os
import glob
import pickle
from pathlib import Path
import json
import yaml
import jsonschema
from jsonschema import validate


__author__ = "Carlos Manuel Molina Sotoca"
__email__ = "cmmolinas01@gmail.com"


ROOT_PATH = str(Path(os.path.dirname(os.path.realpath(__file__))).parent)


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


class FileOperations:
    @staticmethod
    def check_file_exists(file_path):
        if os.path.isfile(file_path):
            return True
        else:
            raise Exception("File does not exist.")

    @staticmethod
    def get_file_name(file_path):
        return os.path.basename(file_path)

    @staticmethod
    def read_file(file_path):
        FileOperations.check_file_exists(file_path)
        with open(file_path, 'r', encoding="utf-8") as file_obj:
            file_content = file_obj.read()
        return file_content

    @staticmethod
    def read_file_lines(file_path):
        FileOperations.check_file_exists(file_path)
        with open(file_path, 'r', encoding="utf-8") as file_obj:
            file_content = file_obj.readlines()
        return file_content

    @staticmethod
    def write_file(file_path, string):
        DirectoryOperations.create_dir_by_file_path(file_path)
        with open(file_path, 'w', encoding="utf-8") as file_obj:
            file_obj.write(string)

    @staticmethod
    def append_text_file(file_path, string):
        FileOperations.check_file_exists(file_path)
        with open(file_path, 'a', encoding="utf-8") as file_obj:
            file_obj.write(string)


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
        parsed = json.loads(json.dumps(dictionary))
        print(json.dumps(parsed, indent=4, sort_keys=True))


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
