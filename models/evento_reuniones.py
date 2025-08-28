from sqlalchemy import Column, Integer, String, Date, Text
from sqlalchemy.orm import relationship
from config.dbconfig import Base

class Evento (Base):
    __tablename__ = "evento"

    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String(255), nullable=False, default="Desconocido")
    tipo = Column(String(255), nullable=False, default="Desconocido")
    agrupacion = Column(String(255), nullable=False, default="Desconocido")
    detalle_tipo_rotacion = Column(String(255), nullable=False, default="Desconocido")
    tema = Column(String(255), nullable=False, default="Otro")
    fecha_edicion = Column(Date, nullable=True)
    fecha_inicio = Column(Date, nullable=True)
    fecha_fin = Column(Date, nullable=True)
    anio = Column(String(255), nullable=False, default="2025")
    mes = Column(String(255), nullable=False, default="Desconocido")
    dia_inicio = Column(String(255), nullable=False, default="Desconocido")
    dia_fin = Column(String(255), nullable=False, default="Desconocido")
    fecha_texto = Column(String(255), nullable=False, default="Desconocida")
    sede = Column(Text, nullable=False, default="Desconocida")
    sitio_web = Column(String(255), nullable=False, default="Desconocido")
    entidad_organizadora = Column(String(255), nullable=False, default="Desconocida")
    requiere_revision = Column(String(255), nullable=False, default=True)