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

RAW_CVE_METROPOLIS {
    int cvegeo PK
    string cve_sun
    string metropoli
}

CVE_METROPOLIS {
    int cvegeo PK
    string cve_sun
    string metropoli
}

RAWCOVAR {
    int cvegeo PK
    string archivo 
    string indice
    int fecha
    float valor
    int anio
    string categoria
}

RAWPOPULATION {
    int cvegeo PK
    int anio
    string sexo
    string edad_gpo
    int poblacion
}
POPULATION {
    int cvegeo PK
    int anio
    string sexo
    string edad_gpo
    int poblacion
}
POPULATION_GENDER {
    int cvegeo PK
    int anio
    string sexo
    int poblacion
}
POPULATION_AGE {
    int cvegeo PK
    int anio
    string edad_gpo
    int poblacion
}
POPULATION_TOTAL {
    int anio
    int cvegeo PK
    int total_population
}

VAR_DISEASES {
    string cve_enfermedad PK
    string enfermedad 
    string id
}
DATA_VAR_DISEASES {
    string id
    string cve_enfermedad PK
    int anio
    int cvegeo
    int count
}

VAR_GROUP {
    string cve_grupo PK
    string grupo
    string id
}
DATA_VAR_GROUP {
    string id
    string cve_grupo PK
    int anio
    int cvegeo
    int count
}

VAR_CAUSEDEATH {
    string cve_causa_def PK
    string causa_def
    string id
}
DATA_VAR_CAUSEDEATH {
    string id
    string cve_causa_def PK
    int anio
    int cvegeo
    int count
}

RAWDATA ||--|| ENFERMEDADES: "hecha desde"
RAWDATA ||--|| ESTADO_MUN: "hecha desde"
RAWDATA ||--|| DEFUNCIONES: "hecha desde"
RAWDATA ||--|| METROPOLI: "hecha desde"

RAW_CVE_METROPOLIS ||--|| CVE_METROPOLIS: "hecha desde"

RAWPOPULATION ||--|| POPULATION: "hecha desde"
RAWPOPULATION ||--|| POPULATION_GENDER: "hecha desde"
RAWPOPULATION ||--|| POPULATION_AGE: "hecha desde"
RAWPOPULATION ||--|| POPULATION_TOTAL: "hecha desde"

RAWDATA ||--|| VAR_DISEASES: "hecha desde"
VAR_DISEASES ||--|| DATA_VAR_DISEASES: "hecha desde"

RAWDATA ||--|| VAR_GROUP: "hecha desde"
VAR_GROUP ||--|| DATA_VAR_GROUP: "hecha desde"

RAWDATA ||--|| VAR_CAUSEDEATH: "hecha desde"
VAR_CAUSEDEATH ||--|| DATA_VAR_CAUSEDEATH: "hecha desde"

```