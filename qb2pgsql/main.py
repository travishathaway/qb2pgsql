import logging
from pathlib import Path
from typing import NamedTuple
from xml.etree import ElementTree as et

import click
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from qb2pgsql.constants import INFORMATION_REPORT_GLOB
from qb2pgsql.db import create_tables, ensure_schema, make_base, make_engine

logger = logging.getLogger(__name__)


class Address(NamedTuple):
    """
    Represent a single address
    """

    ik_number: int
    location_id: int
    street: str
    city: str
    house_number: str
    zip_code: int


class EmergencyMedicalServices(NamedTuple):
    """
    Give information about emergency medical services
    """

    ik_number: int
    location_id: int
    provides_services: bool
    levels: tuple[str, ...] | None


class ReportID(NamedTuple):
    """
    Unique identifiers for a single report
    """

    ik_number: int
    location_id: int


def get_standort(root: et.Element) -> et.Element | None:
    """
    Get the standort from XML tree
    """
    standorte = root.find("Krankenhaus").find("Mehrere_Standorte")
    if standorte is not None:
        standort = standorte.find("Standortkontaktdaten")
    else:
        standort = root.find("Krankenhaus").find("Ein_Standort").find("Krankenhauskontaktdaten")
    return standort


def get_report_id(root: et.Element) -> ReportID | None:
    """
    Get `ReportID` from XML tree
    """
    standort = get_standort(root)
    standort_nummer = standort.find("Standortnummer").text
    ik_nummer = standort.find("IK").text

    return ReportID(int(ik_nummer), int(standort_nummer))


def get_address(root: et.Element) -> Address | None:
    """
    Get the address from XML tree
    """
    try:
        standort = get_standort(root)
        report_id = get_report_id(root)

        zugang = standort.find("Kontakt_Zugang")
        return Address(
            ik_number=report_id.ik_number,
            location_id=report_id.location_id,
            street=zugang.find("Strasse").text,
            city=zugang.find("Ort").text,
            house_number=zugang.find("Hausnummer").text,
            zip_code=int(zugang.find("Postleitzahl").text),
        )
    except Exception as e:
        logger.error(f"Could not parse address from XML for {e}")


def get_emergency_medical_services_info(root: et.Element) -> EmergencyMedicalServices | None:
    """
    Get `Teilnahme_Notfallversorgung` section and map values to an `EmergencyMedicalServices`
    object
    """
    try:
        report_id = get_report_id(root)
        section = root.find("Teilnahme_Notfallversorgung")
        level = section.find("Teilnahme_Notfallstufe")

        # Special-care hospitals (Spezialversorgung) have no Teilnahme_Notfallstufe element
        if level is None:
            return EmergencyMedicalServices(
                ik_number=report_id.ik_number,
                location_id=report_id.location_id,
                provides_services=False,
                levels=None,
            )

        no_services = level.find("Keine_Teilnahme_Notfallversorgung")
        not_yet_arranged = level.find("Notfallstufe_Nichtteilnahme_noch_nicht_vereinbart")
        zugeordnet = level.find("Notfallstufe_zugeordnet")

        if no_services is not None or not_yet_arranged is not None:
            return EmergencyMedicalServices(
                ik_number=report_id.ik_number,
                location_id=report_id.location_id,
                provides_services=False,
                levels=None,
            )
        elif zugeordnet is not None:
            ordered_levels = tuple(elm.tag for elm in zugeordnet)
            return EmergencyMedicalServices(
                ik_number=report_id.ik_number,
                location_id=report_id.location_id,
                provides_services=True,
                levels=ordered_levels,
            )
    except Exception as e:
        logger.error(f"Could not parse emergency medical services from XML: {e}")


def build_hospital(address: Address, ems: EmergencyMedicalServices, hospital_cls):
    return hospital_cls(
        ik_number=address.ik_number,
        location_id=address.location_id,
        street=address.street,
        city=address.city,
        house_number=address.house_number,
        zip_code=address.zip_code,
        provides_emergency_services=ems.provides_services,
        levels=list(ems.levels) if ems.levels is not None else None,
    )


@click.command()
@click.option("--host", default="localhost", show_default=True, help="PostgreSQL host")
@click.option("--port", default=5432, show_default=True, type=int, help="PostgreSQL port")
@click.option("--database", required=True, help="PostgreSQL database name")
@click.option("--user", required=True, help="PostgreSQL user")
@click.option(
    "--password",
    default="",
    hide_input=True,
    envvar="PGPASSWORD",
    help="PostgreSQL password (or set PGPASSWORD env var)",
)
@click.option("--schema", default="public", show_default=True, help="Target schema name")
@click.argument("data_dir", type=click.Path(exists=True, dir_okay=True, file_okay=False))
def main(host, port, database, user, password, schema, data_dir) -> None:
    """
    Import German hospital quality reports (Qualitätsberichte) into PostgreSQL.

    To obtain the dataset and learn more, visit: https://qb-datenportal.g-ba.de/
    """
    Base, Hospital = make_base(schema)
    engine = make_engine(host, port, database, user, password)
    ensure_schema(engine, schema)
    create_tables(engine, Base)

    files = list(Path(data_dir).glob(INFORMATION_REPORT_GLOB))
    hospitals = []

    for file in files:
        try:
            root = et.parse(file).getroot()
            address = get_address(root)
            ems = get_emergency_medical_services_info(root)
            if address is None or ems is None:
                logger.warning(f"Skipping {file}: incomplete data")
                continue
            hospitals.append(build_hospital(address, ems, Hospital))
        except Exception as e:
            logger.error(f"Failed to process {file}: {e}")

    with Session(engine) as session:
        for hospital in hospitals:
            non_pk_cols = {
                c.key: getattr(hospital, c.key)
                for c in Hospital.__table__.columns
                if not c.primary_key
            }
            stmt = (
                pg_insert(Hospital)
                .values(
                    ik_number=hospital.ik_number, location_id=hospital.location_id, **non_pk_cols
                )
                .on_conflict_do_update(
                    index_elements=["ik_number", "location_id"], set_=non_pk_cols
                )
            )
            session.execute(stmt)
        session.commit()
