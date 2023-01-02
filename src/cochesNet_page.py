import json
import ast
from bs4 import BeautifulSoup
from dataclasses import dataclass


@dataclass
class CochesNetPageData:
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:107.0) Gecko/20100101 Firefox/107.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip,deflate,br',
        'Referer': 'https://www.coches.net/segunda-mano/'
    }
    base_url = 'http://www.coches.net/segunda-mano'
    cars_json_identification = "window.__INITIAL_PROPS__ = JSON.parse(\""

    @staticmethod
    def get_url():
        return CochesNetPageData.base_url

    @staticmethod
    def get_headers():
        return CochesNetPageData.headers

    @staticmethod
    def get_url_filter_most_recent(page: int):
        return CochesNetPageData.base_url + '/?pg=' + str(page)


class CochesNetPage(CochesNetPageData):
    def __init__(self):
        pass

    def get_cars_dict_using_html(self, html_file: str) -> dict:
        cars_dict = {}

        html_data = BeautifulSoup(html_file, 'html.parser')
        scripts = html_data.find_all('script')
        for script in scripts:
            script_string = script.text
            if script_string.find(self.cars_json_identification) != -1:
                cars_dict_unformatted = script_string[len(self.cars_json_identification):-3]\
                    .replace('\\\\', '\\').replace('\\"', '"')
                cars_dict = json.loads(cars_dict_unformatted)
        return cars_dict
