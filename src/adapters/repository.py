from sqlalchemy import and_, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import asc, desc, func, text
from src.adapters.orm import (
    Announcement,
    Vehicle,
    Seller
)
from src.utils import ROOT_PATH
from src.utils import YAMLFileOperations
from datetime import datetime
import re


__author__ = "Carlos Manuel Molina Sotoca"
__email__ = "cmmolinas01@gmail.com"


class SqlAlchemyRepository:
    def __init__(self):
        super().__init__()
        self.config = YAMLFileOperations.read_file(ROOT_PATH + '/config.yml')
        self.session = self._create_connection()

    def _create_connection(self):
        database_type = self.config["application"]["database"]
        database_data = self.config["database_data"][database_type]

        # Connect to the database using SQLAlchemy
        if database_type == 'sqlite':
            engine = create_engine(f"sqlite:///{ROOT_PATH + database_data['file']}")
        elif database_type == 'mariadb':
            engine = create_engine(f"mysql+pymysql://"
                                   f"{database_data['user']}:{database_data['password']}"
                                   f"@{database_data['host']}:{database_data['port']}/{database_data['database']}")
        else:
            raise Exception(f'Unknown database type: {database_type}')

        Session = sessionmaker()
        Session.configure(bind=engine)
        session = Session()
        return session

    @staticmethod
    def _value_to_str_for_db(value):
        return str(value).replace("'", "''")

    def insert_row(self, table, data):
        values_str = ""
        value_index = 0
        for key, value in data.items():
            if "DATE" in key:
                datetime_numbers = [int(number) for number in re.split(r'\D+', value)[:-1]]
                value = str(datetime(*datetime_numbers))

            if value is None:
                values_str += "NULL"
            elif type(value) == str:
                format_value = self._value_to_str_for_db(value)
                values_str += f"'{format_value}'"
            else:
                values_str += f"{value}"

            value_index += 1
            if len(data.values()) > value_index:
                values_str += ", "

        query = f"""
            INSERT INTO {table} ({', '.join(data.keys())})
            VALUES ({values_str});
        """
        self.session.execute(text(query))

    def get_announcement_id_by_ad_id(self,
                                     ad_id,
                                     announcer):
        result = (
            self.session.query(
                Announcement.id
            )
            .filter(
                and_(
                    Announcement.announcement_id == ad_id,
                    Announcement.announcer == announcer
                )
            )
            .all()
        )
        return result

    def get_announcement_basic_info(self):
        result = (
            self.session.query(
                Announcement.id,
                Announcement.title,
                Announcement.vehicle_year,
                Announcement.vehicle_km,
                Announcement.price,
                Announcement.announcer
            )
            .all()
        )
        return result

    def get_announcement_id_by_basic_info(self,
                                          title,
                                          vehicle_year,
                                          vehicle_km,
                                          price,
                                          announcer):
        result = (
            self.session.query(
                Announcement.id
            )
            .filter(
                and_(
                    Announcement.title == self._value_to_str_for_db(title),
                    Announcement.vehicle_year == vehicle_year,
                    Announcement.vehicle_km == vehicle_km,
                    Announcement.price == price,
                    Announcement.announcer == announcer
                )
            )
            .all()
        )
        return result

    def get_vehicle_basic_info(self):
        result = (
            self.session.query(
                Vehicle.id,
                Vehicle.make,
                Vehicle.model,
                Vehicle.version,
                Vehicle.year
            )
            .all()
        )
        return result

    def get_vehicle_id_by_basic_info(self,
                                     make,
                                     model,
                                     version,
                                     year):
        _and_query = [
            Vehicle.make == self._value_to_str_for_db(make),
            Vehicle.model == self._value_to_str_for_db(model)
        ]
        if version is not None:
            _and_query += [Vehicle.version == self._value_to_str_for_db(version)]
        if year is not None:
            _and_query += [Vehicle.year == year]

        result = (
            self.session.query(
                Vehicle.id
            )
            .filter(
                and_(*_and_query)
            )
            .all()
        )
        return result

    def get_seller_basic_info(self):
        result = (
            self.session.query(
                Seller.id,
                Seller.name,
                Seller.province
            )
            .all()
        )
        return result

    def get_seller_id_by_basic_info(self,
                                    name,
                                    province):
        _and_query = [
            Seller.name == self._value_to_str_for_db(name)
        ]
        if province is not None:
            _and_query += [Seller.province == self._value_to_str_for_db(province)]

        result = (
            self.session.query(
                Seller.id
            )
            .filter(
                and_(*_and_query)
            )
            .all()
        )
        return result
