import datetime
from sqlalchemy import and_, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import asc, desc, func, text
from src.adapters.orm import (
    Base,
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
        Session.configure(bind=engine, autoflush=True)
        session = Session()
        return session

    def close_connection(self):
        # TODO: implement close connection method
        pass

    @staticmethod
    def _value_to_str_for_db(value):
        return str(value).replace("'", "''")

    def insert_row(self, table, data):
        values_str = ""
        value_index = 0
        for key, value in data.items():
            if "DATE" in key:
                datetime_numbers = [int(number) for number in re.split(r'\D+', str(value))[:-1]]
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
        self.session.commit()

    def _insert_multiple_rows(self, object, data):
        data_mapping = []
        # TODO: convert to dataframe:
        for row in data.to_dict('records'):
            ad_obj = object
            for key, value in row.items():
                setattr(ad_obj, key.lower(), value if value != '' else None)
            data_mapping.append(ad_obj)

        self.session.add_all(data_mapping)
        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            print(f"Error en la transacción: {str(e)}")

        # Return ids:
        return [data_map_obj.id for data_map_obj in data_mapping]

    def insert_announcements(self, data):
        entity_orm = Announcement()
        return self._insert_multiple_rows(entity_orm, data)

    def insert_vehicles(self, data):
        entity_orm = Vehicle()
        return self._insert_multiple_rows(entity_orm, data)

    def insert_sellers(self, data):
        entity_orm = Seller()
        return self._insert_multiple_rows(entity_orm, data)

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
                    Announcement.title == title,
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
            Vehicle.make == make,
            Vehicle.model == model
        ]
        if version is not None:
            _and_query += [Vehicle.version == version]
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
            Seller.name == name
        ]
        if province is not None:
            _and_query += [Seller.province == province]

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


if __name__ == "__main__":
    obj = SqlAlchemyRepository()
    announcement = [{'ANNOUNCEMENT_ID': '54367535',
                    'ANNOUNCER': 'coches.net',
                    'TITLE': 'KIA Sportage 1.6 CRDi 100kW 136CV Concept 4x2 5p.',
                    'DESCRIPTION': 'Talleres de las Heras, concesionario oficial KIA en el Corredor del Henares con exposiciones en Alcalá de Henares, Torrejón de Ardoz y Coslada, vende KIA Sportage 1.6 CRDi 136 CV 4x2 DCT Business de ocasión. Un SUV turbodiésel, con cambio automático de doble embrague y consumo contenido. Consulta condiciones. Disponemos de diferentes descuentos para colectivos y empresas, además de ofertas de renting.\r-Precio al contado: 24.900 €.\r-Precio financiado: 23.700 €.\r-Oferta válida salvo error tipográfico.',
                    'URL': '/kia-sportage-16-crdi-100kw-136cv-concept-4x2-5p-diesel-2019-en-madrid-54367535-covo.aspx',
                    'OFFER_TYPE': 'Ocasión',
                    'VEHICLE_ID': 26641,
                    'VEHICLE_KM': 53787,
                    'VEHICLE_YEAR': 2019,
                    'STATUS': 'active',
                    'VEHICLE_COLOR': 'Gris / Plata (Gris)',
                    'PRICE': 24900,
                    'FINANCED_PRICE': 23700,
                    'HAS_TAXES': True,
                    'WARRANTY_MONTHS': 36,
                    'WARRANTY_OFFICIAL': False,
                    'IS_FINANCED': True,
                    'IS_CERTIFIED': True,
                    'IS_PROFESSIONAL': True,
                    'HAS_URGE': False,
                    'PROVINCE': 'Madrid',
                    'AD_CREATION_DATE': datetime.datetime(2023, 2, 17, 12, 20, 23, 1),
                    'AD_PUBLISHED_DATE': datetime.datetime(2023, 3, 24, 19, 37, 45, 1),
                    'ENVIRONMENTAL_LABEL': 'C',
                    'SELLER_ID': 1271,
                    'CREATED_DATE': datetime.datetime(2023, 3, 25, 9, 59, 36, 524482),
                    'CREATED_USER': 'Ordillan'}]

    obj.insert_announcements(announcement)
