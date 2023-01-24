import json
import ast
from bs4 import BeautifulSoup
from dataclasses import dataclass


@dataclass
class CochesNetAPIData:
    base_url = 'https://ms-mt--api-web.spain.advgo.net'

    headers = {
            'authority': 'ms-mt--api-web.spain.advgo.net',
            'sec-ch-ua': '"Not;ABrand";v="99","GoogleChrome";v="91","Chromium";v="91"',
            'accept': 'application/json;charset=UTF-8',
            'x-adevinta-channel': 'web-desktop',
            'x-schibsted-tenant': 'coches',
            'sec-ch-ua-mobile': '?0',
            'user-agent': 'Mozilla/5.0(X11;Linuxx86_64)AppleWebKit/537.36(KHTML,likeGecko)Chrome/91.0.4472.114Safari/537.36',
            'content-type': 'application/json;charset=UTF-8',
            'origin': 'https://www.coches.net',
            'sec-fetch-site': 'cross-site',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.coches.net/',
            'accept-language': 'en-US,en;q=0.9,es;q=0.8'
        }

    search_default_body = {
            "pagination": {
                "page": 0,
                "size": 100
            },
            "sort": {
                "order": "desc",
                "term": "year"
            },
            "filters": {
                "isFinanced": False,
                "price": {
                    "from": None,
                    "to": None
                },
                "bodyTypeIds": [],
                "categories": {
                    "category1Ids": [
                        2500
                    ]
                },
                "contractId": 0,
                "drivenWheelsIds": [],
                "environmentalLabels": [],
                "equipments": [],
                "fuelTypeIds": [],
                "hasPhoto": None,
                "hasWarranty": None,
                "hp": {
                    "from": None,
                    "to": None
                },
                "isCertified": False,
                "km": {
                    "from": None,
                    "to": None
                },
                "luggageCapacity": {
                    "from": None,
                    "to": None
                },
                "onlyPeninsula": False,
                "offerTypeIds": [
                    0,
                    2,
                    3,
                    4,
                    5
                ],
                "provinceIds": [
                ],
                "sellerTypeId": 0,
                "transmissionTypeId": 0,
                "year": {
                    "from": None,
                    "to": None
                }
            }
        }


class CochesNetAPI(CochesNetAPIData):
    def __init__(self):
        self.url_search_listing = self.base_url + "/search/listing"
        self.url_announcement_detail = self.base_url + "/details/{id}"

    def get_request_search_by_date_desc(self, page: int) -> dict:
        body = self.search_default_body.copy()
        body["pagination"]["page"] = page

        request = {
            "method": "POST",
            "url": self.url_search_listing,
            "headers": self.headers,
            "json": body
        }
        return request

    def get_request_announcement(self, announcement: dict) -> dict:
        request = {
            "method": "GET",
            "url": self.url_announcement_detail.replace('{id}', announcement["id"]),
            "headers": self.headers
        }
        return request

    def get_number_pages(self, response: dict) -> int:
        return response["meta"]["totalResults"]

    def get_announcements(self, response: dict) -> list:
        return response["items"]
