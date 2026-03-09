import logging
import re
import time
from pathlib import Path
from typing import NamedTuple
from xml.etree import ElementTree as et

import click
import httpx
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn, TimeRemainingColumn
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
    zip_code: str

    @property
    def address(self) -> str:
        """Get full address as a single line string"""
        return f"{self.house_number} {self.street}, {self.city}, {self.zip_code}"


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
    hospital_name: str
    ik_number: int
    location_id: int


def get_standort(root: et.Element) -> tuple[et.Element, et.Element | None] | None:
    """
    Get the standort from XML tree
    """
    standorte = root.find("Krankenhaus").find("Mehrere_Standorte")
    if standorte is not None:
        standort = standorte.find("Standortkontaktdaten")
        krankenhaus = standorte.find("Krankenhauskontaktdaten")
    else:
        standort = root.find("Krankenhaus").find("Ein_Standort").find("Krankenhauskontaktdaten")
        krankenhaus = None
    return standort, krankenhaus


def get_report_id(root: et.Element) -> ReportID | None:
    """
    Get `ReportID` from XML tree
    """
    standort, krankenhaus = get_standort(root)
    standort_nummer = standort.find("Standortnummer").text
    ik_nummer = standort.find("IK").text
    name = standort.find("Name").text

    return ReportID(hospital_name=name, ik_number=int(ik_nummer), location_id=int(standort_nummer))


def sanitize_street(street: str) -> str:
    """
    Remove number from street name
    """
    # Remove trailing house number (German style): "Hauptstraße 12b"
    result = re.sub(r'\s+\d+[a-zA-Z]?(?:[-/]\d+[a-zA-Z]?)?$', '', street)

    # Fix typos
    result = result.replace('staße', '')
    result = result.replace('Rober-Koch-Straße', 'Robert-Koch-Straße')

    return result.strip()


def get_address(root: et.Element) -> Address | None:
    """
    Get the address from XML tree

    Some of the files have this weird "/ OT" or "/OT" in the Ort field, which is why we
    remove this substrings.
    """
    try:
        standort, krankenhaus = get_standort(root)

        if krankenhaus is not None:
            standort = krankenhaus

        report_id = get_report_id(root)

        zugang = standort.find("Kontakt_Zugang")

        # This record is just incorrect :sob:
        if standort.find("Name").text == "AMEOS Klinikum Alfeld":
            house_number = "26"
            street = sanitize_street(zugang.find("Strasse").text).replace(
                "Landrat-Beushausen-tr.",
                "Landrat-Beushausen-Straße"
            )
        else:
            house_number = zugang.find("Hausnummer").text.replace(
                "Gerhard-Kienle-Weg ", ""
            ).replace(
                "Gebäude ", ""
            )
            street = sanitize_street(zugang.find("Strasse").text)

        return Address(
            ik_number=report_id.ik_number,
            location_id=report_id.location_id,
            street=street,
            # Somtime these weird characters appear in the city
            city=zugang.find("Ort").text.replace("/OT", "").replace("/ OT", ""),
            house_number=house_number,
            zip_code=zugang.find("Postleitzahl").text,
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


def geocode_address(address: Address) -> tuple[float, float] | None:
    """Look up lat/lon for an address using Nominatim (OpenStreetMap)."""
    try:
        response = httpx.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "street": f"{address.house_number} {address.street}",
                "city": address.city,
                "postalcode": address.zip_code,
                "country": "Germany",
                "format": "json",
                "limit": 1,
            },
            headers={"User-Agent": "example"},
            timeout=10,
        )
        response.raise_for_status()
        results = response.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as e:
        logger.error(f"Geocoding failed for {address.address}: {e}")
    return None


def build_hospital(
    report: ReportID,
    address: Address,
    ems: EmergencyMedicalServices,
    hospital_cls,
    latitude: float | None = None,
    longitude: float | None = None,
):
    return hospital_cls(
        ik_number=report.ik_number,
        location_id=report.location_id,
        name=report.hospital_name,
        street=address.street,
        city=address.city,
        house_number=address.house_number,
        zip_code=address.zip_code,
        address=address.address,
        provides_emergency_services=ems.provides_services,
        levels=list(ems.levels) if ems.levels is not None else None,
        latitude=latitude,
        longitude=longitude,
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
@click.option("--geocode", is_flag=True, default=False, help="Geocode addresses using Nominatim (OpenStreetMap)")
@click.argument("data_dir", type=click.Path(exists=True, dir_okay=True, file_okay=False))
def main(host, port, database, user, password, schema, geocode, data_dir) -> None:
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

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn()
    ) as progress:
        parse_task = progress.add_task("Parsing XML files...", total=len(files))
        for file in files:
            try:
                root = et.parse(file).getroot()
                address = get_address(root)
                report = get_report_id(root)
                ems = get_emergency_medical_services_info(root)
                if address is None or ems is None:
                    logger.warning(f"Skipping {file}: incomplete data")
                    progress.advance(parse_task)
                    continue
                lat, lon = None, None
                if geocode and address is not None:
                    result = geocode_address(address)
                    if result:
                        lat, lon = result
                    time.sleep(1)
                hospitals.append(build_hospital(report, address, ems, Hospital, lat, lon))
            except Exception as e:
                logger.error(f"Failed to process {file}: {e}")
            progress.advance(parse_task)

        insert_task = progress.add_task("Inserting into database...", total=len(hospitals))
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
                progress.advance(insert_task)
            session.commit()
