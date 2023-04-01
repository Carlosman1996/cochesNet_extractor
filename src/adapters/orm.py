import logging
from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    Date,
    Boolean,
    Float,
    ForeignKey
)
from sqlalchemy.orm import (
    relationship,
    backref
)
from sqlalchemy.ext.declarative import declarative_base


logger = logging.getLogger(__name__)

Base = declarative_base()


class Vehicle(Base):
    __tablename__ = "VEHICLE"

    id = Column(Integer, primary_key=True, autoincrement=True)
    make = Column(String)
    model = Column(String)
    version = Column(String)
    year = Column(Integer)
    horse_power = Column(Integer)
    fuel_type = Column(String)
    cubic_capacity = Column(Integer)
    transmission_type = Column(String)
    co2_emissions = Column(Integer)
    environmental_label = Column(String)
    dimension_width = Column(Integer)
    dimension_height = Column(Integer)
    dimension_length = Column(Integer)
    weight = Column(Integer)
    body_type = Column(String)
    number_doors = Column(Integer)
    number_seats = Column(Integer)
    trunk_capacity_liters = Column(Integer)
    tank_capacity_liters = Column(Integer)
    consumption_urban = Column(Float)
    consumption_mixed = Column(Float)
    consumption_extra_urban = Column(Float)
    max_speed = Column(Integer)
    acceleration = Column(Integer)
    manufacturer_price = Column(Integer)
    created_date = Column(Date, nullable=False)
    created_user = Column(Date, nullable=False)


class Seller(Base):
    __tablename__ = "SELLER"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    page_url = Column(String)
    country = Column(String)
    province = Column(String)
    zip_code = Column(String)
    created_date = Column(Date, nullable=False)
    created_user = Column(Date, nullable=False)



class Announcement(Base):
    __tablename__ = "ANNOUNCEMENT"

    id = Column(Integer, primary_key=True, autoincrement=True)
    announcement_id = Column(Integer, nullable=False)
    announcer = Column(String)
    title = Column(String)
    description = Column(String)
    url = Column(String)
    offer_type = Column(String)
    vehicle_id = Column(Integer, ForeignKey(Vehicle.id))
    vehicle_km = Column(Integer)
    vehicle_year = Column(Integer)
    status = Column(String)
    vehicle_color = Column(String)
    price = Column(Integer)
    financed_price = Column(Integer)
    has_taxes = Column(Boolean)
    warranty_months = Column(Integer)
    warranty_official = Column(Boolean)
    is_financed = Column(Boolean)
    is_certified = Column(Boolean)
    is_professional = Column(Boolean)
    has_urge = Column(Boolean)
    country = Column(String)
    province = Column(String)
    ad_creation_date = Column(Date)
    ad_published_date = Column(Date)
    environmental_label = Column(String)
    seller_id = Column(Integer, ForeignKey(Seller.id))
    created_date = Column(Date, nullable=False)
    created_user = Column(Date, nullable=False)

    vehicle = relationship(Vehicle, backref="ANNOUNCEMENT")
    seller = relationship(Seller, backref="ANNOUNCEMENT")
