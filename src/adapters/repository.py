from sqlalchemy import and_, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import asc, desc, func
from src.adapters.orm import (
    Announcement,
    Vehicle,
    Seller
)
from src.utils import ROOT_PATH


def create_connection(database: str):
    # Connect to the database using SQLAlchemy
    database_path = ROOT_PATH + "/bbdd/STATISTICARS.db"
    engine = create_engine(f"sqlite:///{database_path}")

    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()

    return session


class SqlAlchemyRepository:
    def __init__(self, session):
        super().__init__()
        self.session = session

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


session = create_connection("")
obj = SqlAlchemyRepository(session)
print(obj.get_announcement_id_by_ad_id(54177166, "coches.net"))
print(obj.get_announcement_id_by_basic_info("OPEL Grandland 1.2 Turbo GS Line 5p.", 2022, 1, 29900, "coches.net"))
print(obj.get_vehicle_id_by_basic_info("OPEL", "Grandland", None, None))
print(obj.get_seller_id_by_basic_info("Meuri Ocasion", None))
