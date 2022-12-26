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
from src.proxies_finder import ProxiesFinder


random.seed(datetime.now().timestamp())
proxies_finder_obj = ProxiesFinder(max_size=20)
ROOT_PATH = str(Path(os.path.dirname(os.path.realpath(__file__))))


PROXY_TRIES = 10


def utfy_dict(dic):
    if isinstance(dic,unicode):
        return(dic.encode("utf-8"))
    elif isinstance(dic,dict):
        for key in dic:
            dic[key] = utfy_dict(dic[key])
        return(dic)
    elif isinstance(dic,list):
        new_l = []
        for e in dic:
            new_l.append(utfy_dict(e))
        return(new_l)
    else:
        return(dic)


def send_request(url, **kwargs):
    proxies = {
        "http": random.choice(proxies_finder_obj.get_proxies())
    }

    response = requests.get(
        url=url,
        proxies=proxies,
        verify=False,
        **kwargs
    )
    response.encoding = response.apparent_encoding
    print('Response HTTP Status Code: ', response.status_code)
    print('Response HTTP Response Body: ', response.text)
    return response.text


# TODO: CONVERT ENCODING TO UTF-8

for page in range(0, 1):
    url = 'https://www.coches.net/segunda-mano/?pg=' + str(page)
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
    time.sleep(5)

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
            json_data_str_decoded = utfy_dict(json_data_str_decoded)

    json_object = json.loads(json_data_str_decoded)
    JSONFileOperations.write_file(ROOT_PATH + "/outputs/page_" + str(page) + ".json", json_object)
    print(json_object)
