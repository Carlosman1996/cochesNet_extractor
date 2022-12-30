from bs4 import BeautifulSoup as BS
import time
import json
import requests
from datetime import datetime
import random
import os
from pathlib import Path

from pyparsing import unicode

from src.utils import JSONFileOperations
from src.utils import FileOperations
from src.utils import PrintColors
from src.proxies_finder import ProxiesFinder
from src.postman import Postman


random.seed(datetime.now().timestamp())
ROOT_PATH = str(Path(os.path.dirname(os.path.realpath(__file__))))


proxies_finder_obj = ProxiesFinder(codes_filter=["US", "CA", "ES", "FR", "AL", "GB", "IT", "PO"],
                                   anonymity_filter=[1, 2],
                                   max_size=100,
                                   # https_filter=True,
                                   google_filter=True
                                   )
proxies_finder_obj.get_proxies()

PROXY_TRIES = len(proxies_finder_obj.proxies_list)


def send_request(url, headers):
    iteration = 0
    response_content = None

    while iteration < PROXY_TRIES:
        proxy = random.choice(proxies_finder_obj.proxies_list)
        try:
            response = Postman.get_request(
                url=url,
                headers=headers,
                http_proxy=proxy,
                timeout=20,
                status_code_check=200
            )

            response.encoding = response.apparent_encoding
            print('Response HTTP Status Code: ', response.status_code)

            if "Algo en tu navegador nos hizo pensar que eres un bot" in response.text or \
                    "You don't have permission to access /vpns/ on this server." in response.text:
                print(f"{PrintColors.FAIL.value}Response HTTP Response Body: KO - forbidden{PrintColors.ENDC.value}")
                iteration += 1
            else:
                print(f"{PrintColors.OKGREEN.value}Response HTTP Response Body: OK{PrintColors.ENDC.value}")
                response_content = response.text
                break
        except Exception as exception:
            print(f"{PrintColors.FAIL.value}Response HTTP Response Body: KO - {str(exception)}{PrintColors.ENDC.value}")
            iteration += 1
    return response_content


# TODO: CONVERT ENCODING TO UTF-8

for page in range(0, 1):
    url = 'http://www.coches.net/segunda-mano/?pg=' + str(page)
    # url = 'https://www.coches.net/segunda-mano/?fi=Year&or=-1&pg=' + str(page)

    # Get URL
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:107.0) Gecko/20100101 Firefox/107.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.coches.net/segunda-mano/'
    }
    response = send_request(url, headers=headers)
    FileOperations.write_file(ROOT_PATH + "/outputs/page_" + str(page) + ".html", response)
    # time.sleep(5)

    # PROCESS DATA:
    file = FileOperations.read_file(ROOT_PATH + "/outputs/page_" + str(page) + ".html")
    soup = BS(file, 'html.parser')

    for announcement in soup.select('.mt-CardAd'):
        results = [result.text for result in announcement.select('.mt-CardBasic-title')]

    results = soup.find_all('script')
    json_data_str_decoded = dict
    for result in results:
        result_string = result.text
        if result_string.find("__INITIAL_PROPS__") != -1:
            json_data_str = result_string[len("window.__INITIAL_PROPS__ = JSON.parse(\""):-3].replace('\\"', '"').replace('\\\"', '"')
            json_data_str_decoded = json_data_str.encode(encoding='UTF-8', errors='strict')
            # json_data_str_decoded = utfy_dict(json_data_str_decoded)

    json_object = json.loads(json_data_str_decoded)
    JSONFileOperations.write_file(ROOT_PATH + "/outputs/page_" + str(page) + ".json", json_object)
    # print(json_object)
