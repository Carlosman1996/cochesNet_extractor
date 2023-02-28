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
                "order": "asc",
                "term": "relevance"
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
        self.page_name = "coches.net"
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

    def get_announcement_id(self, annnouncement: dict) -> int:
        return annnouncement["id"]

    def get_announcement_summary(self, annnouncement: dict) -> dict:
        return {
            "title": annnouncement["title"],
            "vehicle_year": annnouncement["year"],
            "vehicle_km": annnouncement["km"],
            "price": annnouncement["price"]["amount"]
        }


class CochesNetData:
    def __init__(self):
        pass

    @staticmethod
    def _get_model_value(model, keys):
        try:
            value = model
            for key in keys:
                value = value[key]
            return value
        except:
            return None

    @staticmethod
    def get_transmission_type(transmission_type_id):
        if transmission_type_id == 1:
            return "Automático"
        elif transmission_type_id == 2:
            return "Manual"
        else:
            return f"Unknown transmission type id: {transmission_type_id}"

    @staticmethod
    def get_body_type(body_type_id):
        if body_type_id == 1:
            return "Berlina"
        elif body_type_id == 2:
            return "Coupé"
        elif body_type_id == 3:
            return "Cabrio"
        elif body_type_id == 4:
            return "Familiar"
        elif body_type_id == 5:
            return "Monovolumen"
        elif body_type_id == 6:
            return "4X4 SUV"
        elif body_type_id == 7:
            return "Pick Up"
        else:
            return f"Unknown body type id: {body_type_id}"

    def map_announcement_data(self, web_data):
        return {
            "ANNOUNCEMENT_ID": self._get_model_value(web_data, ["detail", "ad", "id"]),
            "ANNOUNCER": "coches.net",
            "TITLE": self._get_model_value(web_data, ["detail", "ad", "title"]),
            "DESCRIPTION": self._get_model_value(web_data, ["detail", "ad", "description"]),
            "URL": self._get_model_value(web_data, ["detail", "ad", "url"]),
            "OFFER_TYPE": self._get_model_value(web_data, ["search", "offerType", "literal"]),
            "VEHICLE_ID": None, # self._get_model_value(web_data, ["detail", "ad", "id"]),
            "VEHICLE_KM": self._get_model_value(web_data, ["detail", "ad", "vehicle", "km"]),
            "VEHICLE_YEAR": self._get_model_value(web_data, ["detail", "ad", "vehicle", "year"]),
            "STATUS": self._get_model_value(web_data, ["detail", "ad", "status"]),
            "VEHICLE_COLOR": self._get_model_value(web_data, ["detail", "ad", "vehicle", "color"]),
            "PRICE": self._get_model_value(web_data, ["detail", "ad", "price", "price"]),
            "FINANCED_PRICE": self._get_model_value(web_data, ["detail", "ad", "price", "financedPrice"]),
            "HAS_TAXES": self._get_model_value(web_data, ["detail", "ad", "price", "hasTaxes"]),
            "WARRANTY_MONTHS": self._get_model_value(web_data, ["detail", "ad", "vehicle", "warranty", "months"]),
            "WARRANTY_OFFICIAL": self._get_model_value(web_data, ["detail", "ad", "vehicle", "warranty", "isOfficial"]),
            "IS_FINANCED": True if self._get_model_value(web_data, ["detail", "ad", "price", "financedPrice"]) is not None else False,
            "IS_CERTIFIED": self._get_model_value(web_data, ["detail", "ad", "vehicle", "isCertified"]),
            "IS_PROFESSIONAL": True if self._get_model_value(web_data, ["detail", "ad", "professionalSeller", "id"]) is not None else False,
            "HAS_URGE": self._get_model_value(web_data, ["search", "hasUrge"]),
            # "COUNTRY": "Spain",
            "PROVINCE": self._get_model_value(web_data, ["search", "mainProvince"]),
            "AD_CREATION_DATE": self._get_model_value(web_data, ["detail", "ad", "creationDate"]),
            "AD_PUBLISHED_DATE": self._get_model_value(web_data, ["detail", "ad", "publicationDate"]),
            "ENVIRONMENTAL_LABEL": self._get_model_value(web_data, ["detail", "ad", "vehicle", "specs", "environmentalLabel"]),
            "SELLER_ID": None, # self._get_model_value(web_data, ["detail", "ad", "id"]),
            "CREATED_DATE": web_data["scraped_date"],
            "CREATED_USER": "Ordillan",
            "ANNOUNCEMENT": None
        }

    def map_vehicle_data(self, web_data):
        return {
            "MAKE": self._get_model_value(web_data, ["detail", "ad", "vehicle", "specs", "make"]),
            "MODEL": self._get_model_value(web_data, ["detail", "ad", "vehicle", "specs", "model"]),
            "VERSION": self._get_model_value(web_data, ["detail", "ad", "vehicle", "specs", "version"]),
            "YEAR": self._get_model_value(web_data, ["detail", "vehicleSpecs", "year"]),
            "HORSE_POWER": self._get_model_value(web_data, ["detail", "vehicleSpecs", "horsePower"]),
            "FUEL_TYPE": self._get_model_value(web_data, ["search", "fuelType"]),
            "CUBIC_CAPACITY": self._get_model_value(web_data, ["detail", "ad", "vehicle", "specs", "cubicCapacity"]),
            "TRANSMISSION_TYPE": self.get_transmission_type(self._get_model_value(web_data, ["detail", "ad", "vehicle", "specs", "transmissionTypeId"])),
            "CO2_EMISSIONS": self._get_model_value(web_data, ["detail", "ad", "vehicle", "specs", "co2Emissions"]),
            "ENVIRONMENTAL_LABEL": self._get_model_value(web_data, ["detail", "ad", "vehicle", "specs", "environmentalLabel"]),
            "DIMENSION_WIDTH": self._get_model_value(web_data, ["detail", "vehicleSpecs", "dimensionsInMillimeters", "width"]),
            "DIMENSION_HEIGHT": self._get_model_value(web_data, ["detail", "vehicleSpecs", "dimensionsInMillimeters", "height"]),
            "DIMENSION_LENGTH": self._get_model_value(web_data, ["detail", "vehicleSpecs", "dimensionsInMillimeters", "length"]),
            "WEIGHT": self._get_model_value(web_data, ["detail", "vehicleSpecs", "weight"]),
            "BODY_TYPE": self.get_body_type(self._get_model_value(web_data, ["detail", "ad", "vehicle", "specs", "bodyTypeId"])),
            "NUMBER_DOORS": self._get_model_value(web_data, ["detail", "vehicleSpecs", "numberOfDoors"]),
            "NUMBER_SEATS": self._get_model_value(web_data, ["detail", "vehicleSpecs", "numberOfSeats"]),
            "TRUNK_CAPACITY_LITERS": self._get_model_value(web_data, ["detail", "vehicleSpecs", "trunkCapacityInLiters"]),
            "TANK_CAPACITY_LITERS": self._get_model_value(web_data, ["detail", "ad", "tankCapacityInLiters"]),
            "CONSUMPTION_URBAN": self._get_model_value(web_data, ["detail", "vehicleSpecs", "consumption", "urban"]),
            "CONSUMPTION_MIXED": self._get_model_value(web_data, ["detail", "vehicleSpecs", "consumption", "mixed"]),
            "CONSUMPTION_EXTRA_URBAN": self._get_model_value(web_data, ["detail", "vehicleSpecs", "consumption", "extraUrban"]),
            "MAX_SPEED": self._get_model_value(web_data, ["detail", "vehicleSpecs", "maxSpeed"]),
            "ACCELERATION": self._get_model_value(web_data, ["detail", "vehicleSpecs", "acceleration"]),
            "MANUFACTURER_PRICE": self._get_model_value(web_data, ["detail", "vehicleSpecs", "manufacturerPrice"]),
            "CREATED_DATE": web_data["scraped_date"],
            "CREATED_USER": "Ordillan"
        }

    def map_seller_data(self, web_data):
        return {
            "NAME": self._get_model_value(web_data, ["detail", "professionalSeller", "name"]),
            "PAGE_URL": self._get_model_value(web_data, ["detail", "professionalSeller", "externalPageUrl"]),
            # "COUNTRY": "Spain",
            "PROVINCE": self._get_model_value(web_data, ["detail", "professionalSeller", "location", "province"]),
            "ZIP_CODE": self._get_model_value(web_data, ["detail", "professionalSeller", "location", "zipCode"]),
            "CREATED_DATE": web_data["scraped_date"],
            "CREATED_USER": "Ordillan"
        }
