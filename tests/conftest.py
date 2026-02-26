"""Pytest configuration and shared fixtures for qb2pgsql tests."""

import pytest
from pathlib import Path


@pytest.fixture
def data_dir() -> Path:
    """Return the path to the project's data directory."""
    return Path(__file__).parent.parent / "data"


@pytest.fixture
def sample_xml_path(data_dir: Path) -> Path:
    """Return the path to a sample XML file for testing."""
    return data_dir / "260100023-773287000-2024-xml.xml"
