import logging
import random
from glob import glob
from typing import NamedTuple, Literal
from xml.etree import ElementTree as et

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
    provides_services: bool
    levels: tuple[str, ...] | None


def get_address(root: et.Element) -> Address | None:
    """
    Get the address from an XML tree
    """
    try:
        standorte = root.find("Krankenhaus").find("Mehrere_Standorte")
        if standorte is not None:
            standort = standorte.find("Standortkontaktdaten")
        else:
            standort = root.find("Krankenhaus").find("Ein_Standort").find("Krankenhauskontaktdaten")

        standort_nummer = standort.find("Standortnummer").text
        ik_nummer = standort.find("IK").text

        zugang = standort.find("Kontakt_Zugang")
        return Address(
            ik_number=int(ik_nummer),
            location_id=int(standort_nummer),
            street=zugang.find("Strasse").text,
            city=zugang.find("Ort").text,
            house_number=zugang.find("Hausnummer").text,
            zip_code=int(zugang.find("Postleitzahl").text)
        )
    except Exception as e:
        logger.error(f"Could not parse address from XML for {e}")


def get_emergency_medical_services_info(root: et.Element) -> EmergencyMedicalServices | None:
    """
    Inspect `Teilnahme_Notfallversorgung` section and map values to an `EmergencyMedicalServices`
    object
    """
    try:
        section = root.find("Teilnahme_Notfallversorgung")
        level = section.find("Teilnahme_Notfallstufe")
        no_services = level.find("Keine_Teilnahme_Notfallversorgung")

        if no_services is not None:
            return EmergencyMedicalServices(
                provides_services=False,
                levels=None
            )
        else:
            ordered_levels = tuple(
                elm.tag for elm in level.find("Notfallstufe_zugeordnet")
            )
            return EmergencyMedicalServices(
                provides_services=True,
                levels=ordered_levels
            )
    except Exception as e:
        logger.error(f"Could not parse emergency medical services from XML: {e}")


def main():
    """
    Parse XML files and grab the addresses and information about whether the hospital
    provides emergency medical services (Teilnahme_Notfallversorgung)
    :return:
    """
    files = glob("./data/*-xml.xml")

    random.seed(1)
    random_files = random.choices(files, k=100)

    for file in random_files:
        print(f"Processing {file}")
        tree = et.parse(file)
        root = tree.getroot()
        # print(get_address(root))
        print(get_emergency_medical_services_info(root))


if __name__ == "__main__":
    main()
