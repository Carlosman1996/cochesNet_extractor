import logging
from sqlalchemy import (
    Table,
    MetaData,
    Column,
    Integer,
    String,
    Date,
    Boolean,
    Float,
    ForeignKey
)

logger = logging.getLogger(__name__)

metadata_obj = MetaData()

announcement = Table(
    "ANNOUNCEMENT",
    metadata_obj,
    Column("ID", Integer, primary_key=True, autoincrement=True),
    Column("ANNOUNCEMENT_ID", Integer, nullable=False),
    Column("ANNOUNCER", Integer, String(500)),
    Column("TITLE", Integer, String(500)),
    Column("DESCRIPTION", Integer, String(5000)),
    Column("URL", Integer, String(500)),
    Column("OFFER_TYPE", Integer, String(100)),
    Column("VEHICLE_ID", Integer, ForeignKey("VEHICLE.ID")),
    Column("VEHICLE_KM", Integer),
    Column("VEHICLE_YEAR", Integer),
    Column("STATUS", Integer, String(100)),
    Column("VEHICLE_COLOR", Integer, String(100)),
    Column("PRICE", Integer),
    Column("FINANCED_PRICE", Integer),
    Column("HAS_TAXES", Integer, Boolean),
    Column("WARRANTY_MONTHS", Integer),
    Column("WARRANTY_OFFICIAL", Integer, Boolean),
    Column("IS_FINANCED", Integer, Boolean),
    Column("IS_CERTIFIED", Integer, Boolean),
    Column("IS_PROFESSIONAL", Integer, Boolean),
    Column("HAS_URGE", Integer, Boolean),
    Column("COUNTRY", Integer, String(100)),
    Column("PROVINCE", Integer, String(100)),
    Column("AD_CREATION_DATE", Integer, Date),
    Column("AD_PUBLISHED_DATE", Integer, Date),
    Column("ENVIRONMENTAL_LABEL", Integer, String(10)),
    Column("SELLER_ID", Integer, ForeignKey("SELLER.ID")),
    Column("CREATED_DATE", Integer, Date, nullable=False),
    Column("CREATED_USER", Integer, Date, nullable=False)
)

vehicle = Table(
    "VEHICLE",
    metadata_obj,
    Column("ID", Integer, primary_key=True, autoincrement=True),
    Column("MAKE", String(100)),
    Column("MODEL", String(500)),
    Column("VERSION", String(1000)),
    Column("YEAR", Integer),
    Column("HORSE_POWER", Integer),
    Column("FUEL_TYPE", String(1000)),
    Column("CUBIC_CAPACITY", Integer),
    Column("TRANSMISSION_TYPE", String(1000)),
    Column("CO2_EMISSIONS", Integer),
    Column("ENVIRONMENTAL_LABEL", String(10)),
    Column("DIMENSION_WIDTH", Integer),
    Column("DIMENSION_HEIGHT", Integer),
    Column("DIMENSION_LENGTH", Integer),
    Column("WEIGHT", Integer),
    Column("BODY_TYPE", String(10)),
    Column("NUMBER_DOORS", Integer),
    Column("NUMBER_SEATS", Integer),
    Column("TRUNK_CAPACITY_LITERS", Integer),
    Column("TANK_CAPACITY_LITERS", Integer),
    Column("CONSUMPTION_URBAN", Float),
    Column("CONSUMPTION_MIXED", Float),
    Column("CONSUMPTION_EXTRA_URBAN", Float),
    Column("MAX_SPEED", Integer),
    Column("ACCELERATION", Integer),
    Column("MANUFACTURER_PRICE", Integer),
    Column("CREATED_DATE", Date, nullable=False),
    Column("CREATED_USER", Date, nullable=False)
)

seller = Table(
    "SELLER",
    metadata_obj,
    Column("ID", Integer, primary_key=True, autoincrement=True),
    Column("NAME", String(500), nullable=False),
    Column("PAGE_URL", String(500)),
    Column("COUNTRY", String(100)),
    Column("PROVINCE", String(100)),
    Column("ZIP_CODE", String(50)),
    Column("CREATED_DATE", Date, nullable=False),
    Column("CREATED_USER", Date, nullable=False)
)
