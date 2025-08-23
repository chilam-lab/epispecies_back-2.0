# API with duckdb and fastapi

To run this project make sure to install the requirements with:
```
pip install -r requirements.txt
```

### to run locally
```
uvicorn main:app --reload
```


### To run the tests
```
PYTHONPATH=. pytest tests/tests.py -v
```


## Here is the BD diagram structure, in order to use this project you need to have a similar structure
```mermaid
---
title: BD DDiagram
---
erDiagram
RAWDATA {
    int CVE_Enfermedad FK
    string Enfermedad
    int CVE_Grupo FK
    string Grupo
    string CVE_Causa_def FK
    string Causa_def
    int CVE_Estado FK
    string Estado
    string CVEGEO FK
    string Municipio
    float Longitud
    float Latitud
    string CVE_Metropoli FK
    string Metropolis
    string Ambito
    int Sexo
    string Edad_gpo
    int Ocupacion
    int Escolaridad
    int Edo_civil
    int Dia
    int Mes
    int Anio
    int Hora
}

ESTADO_MUN {
    int cve_estado PK
    string estado
    string cvegeo PK
    string municipio
}

ENFERMEDADES {
    int id_enfermedad PK
    int CVE_Enfermedad FK
    string Enfermedad
    int CVE_Grupo FK
    string Grupo
    string CVE_Causa_def FK
    int Causa_def
}
DEFUNCIONES {
    int CVE_Enfermedad FK
    int CVE_Grupo FK
    string CVE_Causa_def FK
    int CVE_Estado FK
    string CVEGEO FK
    string CVE_Metropoli FK
    string Ambito
    int Sexo
    string Edad_gpo
    int Ocupacion
    int Escolaridad
    int Edo_civil
    int Anio
}
METROPOLI {
    string CVE_Metropoli FK
    string Metropolis
}
RAWDATA ||--|| ENFERMEDADES: "hecha desde"
RAWDATA ||--|| ESTADO_MUN: "hecha desde"
RAWDATA ||--|| DEFUNCIONES: "hecha desde"
RAWDATA ||--|| METROPOLI: "hecha desde"

RAWPOPULATION {
    string CVEGEO FK
    string Anio FK
    string Sexo FK
    string Edad_gpo FK
    int Poblacion 
}

POPULATION {
    string CVEGEO FK
    string Anio FK
    string Sexo FK
    string Edad_gpo FK
    int Poblacion 
}

POPULATION_AGE {
    string CVEGEO FK
    string Anio FK
    string Edad_gpo FK
    int Poblacion 
}

POPULATION_GENDER {
    string CVEGEO FK
    string Anio FK
    string Sexo FK
    int Poblacion 
}


RAWPOPULATION ||--|| POPULATION: "hecha desde"
RAWPOPULATION ||--|| POPULATION_GENDER: "hecha desde"
RAWPOPULATION ||--|| POPULATION_AGE: "hecha desde"

```
