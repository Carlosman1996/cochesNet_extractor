from bs4 import BeautifulSoup as BS
import time
import json
import requests
import random

from scrapingbee import ScrapingBeeClient
import os
from pathlib import Path
from utils import JSONFileOperations
from utils import FileOperations


ROOT_PATH = str(Path(os.path.dirname(os.path.realpath(__file__))))


PROXY_TRIES = 1
ip_addresses = [
    "mysuperproxy.com:5000",
    "mysuperproxy.com:5001",
    "mysuperproxy.com:5100",
    "mysuperproxy.com:5010",
    "mysuperproxy.com:5050",
    "mysuperproxy.com:8080",
    "mysuperproxy.com:8001",
    "mysuperproxy.com:8000",
    "mysuperproxy.com:8050"
]


def proxy_request(url, **kwargs):
    iteration = 0
    while iteration < PROXY_TRIES:
        try:
            proxy = random.randint(0, len(ip_addresses) - 1)
            proxies = {"http": ip_addresses(proxy), "https": ip_addresses(proxy)}
            response = requests.get(url, proxies=proxies, timeout=5, **kwargs, verify=False)
            print(f"Proxy currently being used: {proxy['https']}")
            break
        except:
            iteration += 1
            print("Error, looking for another proxy")
    return response


def send_request(url, **kwargs):
    proxies = {
        "http": "http://4BU9L424Z752UPQA0AVR57WII9KV98AS1DJMYHWMFV8R97TKAEMC2PIXGDUG5OKV1HCDCEAG915HIQPM:render_js=False&premium_proxy=True@proxy.scrapingbee.com:8886",
        "https": "https://4BU9L424Z752UPQA0AVR57WII9KV98AS1DJMYHWMFV8R97TKAEMC2PIXGDUG5OKV1HCDCEAG915HIQPM:render_js=False&premium_proxy=True@proxy.scrapingbee.com:8887"
    }

    response = requests.get(
        url=url,
        proxies=proxies,
        verify=False,
        **kwargs
    )
    print('Response HTTP Status Code: ', response.status_code)
    print('Response HTTP Response Body: ', response.content)
    return response


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
    FileOperations.write_file(ROOT_PATH + "/outputs/page_" + str(page) + ".html", response.text)
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
    print(json_data_str_decoded)
    json_object = json.loads(json_data_str_decoded)
    JSONFileOperations.write_file(ROOT_PATH + "/outputs/page_" + str(page) + ".json", json_object)
