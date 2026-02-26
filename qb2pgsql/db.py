from sqlalchemy import BigInteger, Boolean, Engine, Integer, Text, create_engine, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session


def make_base(schema: str):
    """Return (Base, Hospital) ORM pair bound to the given schema."""

    class Base(DeclarativeBase):
        pass

    class Hospital(Base):
        __tablename__ = "hospitals"
        __table_args__ = {"schema": schema}

        ik_number: Mapped[int] = mapped_column(BigInteger, primary_key=True)
        location_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
        # Address fields
        street: Mapped[str] = mapped_column(Text, nullable=False)
        city: Mapped[str] = mapped_column(Text, nullable=False)
        house_number: Mapped[str] = mapped_column(Text, nullable=False)
        zip_code: Mapped[int] = mapped_column(Integer, nullable=False)
        # EmergencyMedicalServices fields
        provides_emergency_services: Mapped[bool] = mapped_column(Boolean, nullable=False)
        levels: Mapped[list[str] | None] = mapped_column(ARRAY(Text), nullable=True)

    return Base, Hospital


def make_engine(host: str, port: int, database: str, user: str, password: str) -> Engine:
    url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url, echo=False)


def ensure_schema(engine: Engine, schema: str) -> None:
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.commit()


def create_tables(engine: Engine, base) -> None:
    base.metadata.create_all(engine)
