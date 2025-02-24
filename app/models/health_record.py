from dataclasses import dataclass
from datetime import time
from typing import Optional

@dataclass
class Record:
    Ambito: Optional[str] = None
    Anio: Optional[int] = None
    CVEGEO: Optional[int] = None
    CVE_Causa_def: Optional[str] = None
    CVE_Enfermedad: Optional[int] = None
    CVE_Estado: Optional[int] = None
    CVE_Grupo: Optional[int] = None
    CVE_Metropoli: Optional[str] = None
    Causa_def: Optional[str] = None
    Dia: Optional[int] = None
    Edad_gpo: Optional[int] = None
    Edo_civil: Optional[int] = None
    Enfermedad: Optional[str] = None
    Escolaridad: Optional[int] = None
    Estado: Optional[str] = None
    Grupo: Optional[str] = None
    Hora: Optional[int] = None
    Longitud: Optional[float] = None
    Mes: Optional[int] = None
    Metropolis: Optional[str] = None
    Municipio: Optional[str] = None
    Ocupacion: Optional[int] = None
    Sexo: Optional[int] = None
    latitud: Optional[float] = None