"""Tests for qb2pgsql.main module."""

import pytest
from xml.etree import ElementTree as et

from qb2pgsql.main import (
    Address,
    EmergencyMedicalServices,
    get_address,
    get_emergency_medical_services_info,
)


# ---------------------------------------------------------------------------
# Unit tests — pure parsing logic against in-memory XML strings
# ---------------------------------------------------------------------------

NO_SERVICES_XML = """<Root>
    <Krankenhaus>
        <Mehrere_Standorte>
            <Standortkontaktdaten>
                <IK>123456789</IK>
                <Standortnummer>1</Standortnummer>
            </Standortkontaktdaten>
        </Mehrere_Standorte>
    </Krankenhaus>
    <Teilnahme_Notfallversorgung>
        <Teilnahme_Notfallstufe>
            <Keine_Teilnahme_Notfallversorgung/>
        </Teilnahme_Notfallstufe>
    </Teilnahme_Notfallversorgung>
</Root>"""

WITH_LEVELS_XML = """<Root>
    <Krankenhaus>
        <Mehrere_Standorte>
            <Standortkontaktdaten>
                <IK>123456789</IK>
                <Standortnummer>1</Standortnummer>
            </Standortkontaktdaten>
        </Mehrere_Standorte>
    </Krankenhaus>
    <Teilnahme_Notfallversorgung>
        <Teilnahme_Notfallstufe>
            <Notfallstufe_zugeordnet>
                <Basisnotfallversorgung/>
                <Erweiterte_Notfallversorgung/>
            </Notfallstufe_zugeordnet>
        </Teilnahme_Notfallstufe>
    </Teilnahme_Notfallversorgung>
</Root>"""

MALFORMED_XML = """<Root>
    <SomeOtherSection/>
</Root>"""

EMPTY_SECTION_XML = """<Root>
    <Teilnahme_Notfallversorgung/>
</Root>"""


def _parse(xml_str: str) -> et.Element:
    return et.fromstring(xml_str)


class TestGetEmergencyMedicalServicesInfo:
    def test_no_services_returns_false_with_none_levels(self):
        root = _parse(NO_SERVICES_XML)
        result = get_emergency_medical_services_info(root)
        assert result == EmergencyMedicalServices(
            ik_number=123456789, location_id=1, provides_services=False, levels=None
        )

    def test_with_levels_returns_correct_tags(self):
        root = _parse(WITH_LEVELS_XML)
        result = get_emergency_medical_services_info(root)
        assert result is not None
        assert result.provides_services is True
        assert result.levels == ("Basisnotfallversorgung", "Erweiterte_Notfallversorgung")

    def test_malformed_xml_returns_none(self):
        root = _parse(MALFORMED_XML)
        result = get_emergency_medical_services_info(root)
        assert result is None

    def test_empty_section_returns_none(self):
        root = _parse(EMPTY_SECTION_XML)
        result = get_emergency_medical_services_info(root)
        assert result is None


# ---------------------------------------------------------------------------
# Data model tests — verify NamedTuple immutability
# ---------------------------------------------------------------------------


class TestDataModels:
    def test_address_is_immutable(self):
        addr = Address(
            ik_number=123456789,
            location_id=1,
            street="Musterstraße",
            city="Berlin",
            house_number="1",
            zip_code=10115,
        )
        with pytest.raises(AttributeError):
            addr.city = "Hamburg"  # type: ignore[misc]

    def test_emergency_medical_services_is_immutable(self):
        ems = EmergencyMedicalServices(
            ik_number=123456789,
            location_id=1,
            provides_services=True,
            levels=("Basisnotfallversorgung",),
        )
        with pytest.raises(AttributeError):
            ems.provides_services = False  # type: ignore[misc]

    def test_address_fields(self):
        addr = Address(
            ik_number=123456789,
            location_id=42,
            street="Testweg",
            city="München",
            house_number="7a",
            zip_code=80331,
        )
        assert addr.ik_number == 123456789
        assert addr.location_id == 42
        assert addr.zip_code == 80331


# ---------------------------------------------------------------------------
# Integration tests — real XML files, skipped if data/ directory is absent
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not __import__("pathlib").Path("data").exists(), reason="data/ directory not present"
)
class TestIntegration:
    def test_sample_xml_parses_emergency_services(self, sample_xml_path):
        tree = et.parse(sample_xml_path)
        root = tree.getroot()
        result = get_emergency_medical_services_info(root)
        assert result is not None
        assert isinstance(result, EmergencyMedicalServices)
        assert isinstance(result.provides_services, bool)

    def test_sample_xml_parses_address(self, sample_xml_path):
        tree = et.parse(sample_xml_path)
        root = tree.getroot()
        result = get_address(root)
        assert result is not None
        assert isinstance(result, Address)
        assert isinstance(result.ik_number, int)
        assert isinstance(result.zip_code, int)
