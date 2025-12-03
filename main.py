from fastapi import Depends, FastAPI, HTTPException, UploadFile, File, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.background import BackgroundTasks
from contextlib import contextmanager
from typing import Generator
from fastapi.encoders import jsonable_encoder
from services.clean_csv import clean_csv_in_chunks, db_columns_to_lowercase
from services.file_helper_functions import get_csv_in_directory_to_clean
from pandas import qcut, read_sql, DataFrame
import os
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv
from io import StringIO
import tempfile
import json
import time
from typing import Annotated

from os import listdir, remove
from os.path import join, exists
import duckdb

load_dotenv()

tags_metadata = [
    {
        "name": "Show",
        "description": "View this project tables, columns and data, to better visualize data in the project.",
    },
    {
        "name": "Column",
        "description": "Related to endpoints that get values in columns.",
    },
    {
        "name": "Level",
        "description": "Get level class of a table.",
    },
    {
        "name": "Population",
        "description": "Related to functions that reference the population, such as the number of population.",
    },
    {
        "name": "Record by year",
        "description": "Get records in a specific year.",
    },
    {
        "name": "Covariables",
        "description": "Related to functions that use covariables, such as calculations and categories.",
    },
    {
        "name": "Project variables",
        "description": "View this project variables for usage in other related projects.",
    },
    {
        "name": "Upload CSV file",
        "description": "A user can upload a CSV file for cleaning and changing its encoding to UTF-8.",
    }
]

app = FastAPI(
    title="Epispecies Backend API",
    description="A collection of useful functions and endpoints to create, calculate and organize geographical data.",
    openapi_tags=tags_metadata
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DuckDBConn = duckdb.DuckDBPyConnection
db_connection = None

id_first_class = os.getenv("ID_FIRST_CLASS")
first_class_description = os.getenv("FIRST_CLASS_DESCRIPTION")
id_second_class = os.getenv("ID_SECOND_CLASS")
second_class_description = os.getenv("SECOND_CLASS_DESCRIPTION")
id_third_class = os.getenv("ID_THIRD_CLASS")
third_class_description = os.getenv("THIRD_CLASS_DESCRIPTION")
table_class = os.getenv("TABLE_CLASS")
table_records = os.getenv("TABLE_RECORDS")

@contextmanager
def get_db_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    con = duckdb.connect("duckdb_files/my_database.db")
    try:
        db_connection.sql("SET threads TO 8;")
        db_connection.sql("SET memory_limit = '8GB';")
        db_connection.sql("PRAGMA enable_parallelism;")
        yield con
    finally:
        con.close()

def get_db() -> duckdb.DuckDBPyConnection:
    yield db_connection

def init_db():
    try:
        if get_csv_in_directory_to_clean():
            db_connection.sql("""
                COPY (SELECT *, CAST(CVEGEO AS VARCHAR) AS CVEGEO 
                FROM read_csv_auto('cleanedCSV/*_db.csv', auto_detect=true, header=true))
                TO 'duckdb_files/RAWDATA.parquet' (FORMAT PARQUET);
            """)
            db_connection.sql("""
                CREATE OR REPLACE TABLE RAWDATA AS
                SELECT * FROM 'duckdb_files/RAWDATA.parquet';
            """)
            db_columns_to_lowercase("RAWDATA", db_connection)

            #COVAR table creation
            #########################
            db_connection.sql("""
                COPY (SELECT *, CAST(CVEGEO AS VARCHAR) AS CVEGEO 
                FROM read_csv_auto('cleanedCSV/*_cov.csv', auto_detect=true, header=true))
                TO 'duckdb_files/RAWCOVAR.parquet' (FORMAT PARQUET);
            """)
            db_connection.sql("""
                CREATE OR REPLACE TABLE CATEGORIES AS
                SELECT * FROM 'duckdb_files/RAWCOVAR.parquet';
            """)
            db_columns_to_lowercase("CATEGORIES", db_connection)


            
            #########################


            #POPULATION table creation
            #########################
            db_connection.sql("""
                COPY (SELECT *, CAST(CVEGEO AS VARCHAR) AS CVEGEO 
                FROM read_csv_auto('cleanedCSV/*_pop.csv', auto_detect=true, header=true))
                TO 'duckdb_files/RAWPOPULATION.parquet' (FORMAT PARQUET);
            """)
            db_connection.sql("""
                CREATE OR REPLACE TABLE RAWPOPULATION AS
                SELECT * FROM 'duckdb_files/RAWPOPULATION.parquet';
            """)
            db_columns_to_lowercase("RAWPOPULATION", db_connection)

            db_connection.sql("""
                CREATE OR REPLACE TABLE POPULATION AS
                SELECT DISTINCT CAST(CVEGEO AS VARCHAR) AS CVEGEO, Anio, Sexo, Edad_gpo, Poblacion
                FROM RAWPOPULATION;
            """)
            db_columns_to_lowercase("POPULATION", db_connection)

            db_connection.sql("""
                CREATE OR REPLACE TABLE POPULATION_GENDER AS
                SELECT DISTINCT CAST(CVEGEO AS VARCHAR) AS CVEGEO, Anio, Sexo, Poblacion
                FROM RAWPOPULATION;
            """)
            db_columns_to_lowercase("POPULATION_GENDER", db_connection)

            db_connection.sql("""
                CREATE OR REPLACE TABLE POPULATION_AGE AS
                SELECT DISTINCT CAST(CVEGEO AS VARCHAR) AS CVEGEO, Anio, Edad_gpo, Poblacion
                FROM RAWPOPULATION;
            """)
            db_columns_to_lowercase("POPULATION_AGE", db_connection)

            db_connection.sql("""
                CREATE OR REPLACE TABLE POPULATION_TOTAL AS
                SELECT 
                    Anio,
                    CAST(CVEGEO AS VARCHAR) AS CVEGEO,
                    SUM(Poblacion) AS Total_Population
                FROM RAWPOPULATION
                GROUP BY Anio, CVEGEO;
            """)
            db_columns_to_lowercase("POPULATION_TOTAL", db_connection)
            ######################

            #METROPOLI table creation
            #########################
            db_connection.sql("""
                COPY (SELECT *, CAST(CVEGEO AS VARCHAR) AS CVEGEO 
                FROM read_csv_auto('cleanedCSV/*_mps.csv', auto_detect=true, header=true))
                TO 'duckdb_files/RAW_CVE_METROPOLIS.parquet' (FORMAT PARQUET);
            """)
            db_connection.sql("""
                CREATE OR REPLACE TABLE RAW_CVE_METROPOLI AS
                SELECT * FROM 'duckdb_files/RAW_CVE_METROPOLIS.parquet';
            """)
            db_columns_to_lowercase("RAW_CVE_METROPOLI", db_connection)

            db_connection.sql("""
                CREATE OR REPLACE TABLE CVE_METROPOLI AS
                SELECT DISTINCT CAST(CVEGEO AS VARCHAR) AS CVEGEO, CVE_SUN AS Cve_Metropoli, Metropoli
                FROM RAW_CVE_METROPOLI;
            """)
            db_columns_to_lowercase("CVE_METROPOLI", db_connection)

            ###########################

            db_connection.sql("""
                CREATE OR REPLACE TABLE ENFERMEDADES AS
                SELECT DISTINCT CVE_Grupo, Grupo, CVE_Enfermedad, Enfermedad, CVE_Causa_def, Causa_def
                FROM RAWDATA;
            """)
            db_columns_to_lowercase("ENFERMEDADES", db_connection)

            #VAR_DISEASES table creation
            ###########################
            db_connection.sql("""
                CREATE OR REPLACE TABLE VAR_DISEASES AS
                SELECT DISTINCT CVE_Enfermedad, Enfermedad || ' ' || Anio AS Enfermedad, 'EN' || CVE_Enfermedad || LPAD(CAST(Anio % 100 AS VARCHAR), 2, '00') AS id
                FROM RAWDATA;
            """)
            db_columns_to_lowercase("VAR_DISEASES", db_connection)

            db_connection.sql("""
                CREATE OR REPLACE TABLE DATA_VAR_DISEASES AS
                SELECT DISTINCT 'EN' || CVE_Enfermedad || LPAD(CAST(Anio % 100 AS VARCHAR), 2, '00') AS id,
                CVE_Enfermedad, Anio, CAST(CVEGEO AS VARCHAR) AS CVEGEO, COUNT(*) AS count
                FROM RAWDATA GROUP BY id, CVE_Enfermedad, Anio, CVEGEO;
            """)
            db_columns_to_lowercase("DATA_VAR_DISEASES", db_connection)

            ###########################

            #VAR_GROUP table creation
            ###########################
            db_connection.sql("""
                CREATE OR REPLACE TABLE VAR_GROUP AS
                SELECT DISTINCT CVE_Grupo, Grupo || ' ' || Anio AS Grupo,
                'GR' || CVE_ENFERMEDAD || CVE_GRUPO || LPAD(CAST(Anio % 100 AS VARCHAR), 2, '00') AS id
                FROM RAWDATA;
            """)
            db_columns_to_lowercase("VAR_GROUP", db_connection)

            db_connection.sql("""
                CREATE OR REPLACE TABLE DATA_VAR_GROUP AS
                SELECT DISTINCT 'GR' || CVE_ENFERMEDAD || CVE_GRUPO || LPAD(CAST(Anio % 100 AS VARCHAR), 2, '00') AS id,
                CVE_Grupo, Anio, CAST(CVEGEO AS VARCHAR) AS CVEGEO, COUNT(*) AS count
                FROM RAWDATA GROUP BY id, CVE_Grupo, Anio, CVEGEO;
            """)
            db_columns_to_lowercase("DATA_VAR_GROUP", db_connection)

            ###########################

            #VAR_CAUSEDEATH table creation
            ###########################
            db_connection.sql("""
                CREATE OR REPLACE TABLE VAR_CAUSEDEATH AS
                SELECT DISTINCT CVE_Causa_def, Causa_def || ' ' || Anio AS Causa_def,
                CVE_Causa_def || LPAD(CAST(Anio % 100 AS VARCHAR), 2, '00') AS id
                FROM RAWDATA;
            """)
            db_columns_to_lowercase("VAR_CAUSEDEATH", db_connection)

            db_connection.sql("""
                CREATE OR REPLACE TABLE DATA_VAR_CAUSEDEATH AS
                SELECT DISTINCT CVE_Causa_def || LPAD(CAST(Anio % 100 AS VARCHAR), 2, '00') AS id,
                CVE_Causa_def, Anio, CAST(CVEGEO AS VARCHAR) AS CVEGEO, COUNT(*) AS count
                FROM RAWDATA GROUP BY id, CVE_Causa_def, Anio, CVEGEO;
            """)
            db_columns_to_lowercase("DATA_VAR_CAUSEDEATH", db_connection)
            ###########################

            db_connection.sql("CREATE INDEX IF NOT EXISTS id_enfermedad ON ENFERMEDADES (CVE_Enfermedad, CVE_Grupo, CVE_Causa_def);")
            db_connection.sql("""
                CREATE OR REPLACE TABLE DEFUNCIONES AS
                SELECT CVE_Enfermedad, CVE_Grupo, CVE_Causa_def, CVE_Estado,
                CAST(CVEGEO AS VARCHAR) AS CVEGEO, CVE_Metropoli, Ambito, Sexo, Edad_gpo, Ocupacion, Escolaridad, Edo_civil, Anio
                FROM RAWDATA;
            """)
            db_columns_to_lowercase("DEFUNCIONES", db_connection)

            db_connection.sql("""
                CREATE OR REPLACE TABLE ESTADO_MUN AS
                SELECT DISTINCT CVE_Estado, Estado, CAST(CVEGEO AS VARCHAR) AS CVEGEO, Municipio
                FROM RAWDATA;
            """)
            db_columns_to_lowercase("ESTADO_MUN", db_connection)
    except Exception as e:
        print(f"Error creating table: {e}")
        tables = db_connection.sql("SHOW TABLES").fetchall()
        print("Available tables:", [row[0] for row in tables])

@app.on_event("startup")
async def startup_event():
    global db_connection
    db_connection = duckdb.connect("duckdb_files/my_database.db")
    init_db()

@app.on_event("shutdown")
async def shutdown_event():
    global db_connection
    if db_connection:
        db_connection.close()

@app.get("/")
async def root(con: DuckDBConn = Depends(get_db)):
    return {"message": "Hello World"}

@app.get("/show/tables", tags=["Show"], summary="Show project tables.")
async def show_tables(con: DuckDBConn = Depends(get_db)):
    """
    Display all created tables in the project.

    *Response*

    A dictionary of all tables.
    """
    try:
        tables = con.sql("SHOW TABLES")
        result = tables.to_df().to_dict(orient="records")
        return {"tables": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/show/columns", tags=["Show"], summary="Show columns of table.")
async def get_columns(table_name:str, con: DuckDBConn = Depends(get_db)):
    """
    Display all columns of a given table.

    *Params*

    table_name: name of table to query.

    *Response*

    A JSON of all columns in the table.
    """
    try:
        rel = con.sql(f"DESCRIBE {table_name}")
        column_names = [row[0] for row in rel.fetchall()]
        return jsonable_encoder({"columns": column_names})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/unique_pair_columns", tags=["Column"], summary="Get unique values between two columns.")
async def get_unique_pair_columns(column1: str, column2: str, table: str, con: DuckDBConn = Depends(get_db)):
    """
    Get all unique values between two columns in a table.

    *Params*

    column1: name of the first column in lower case. \n
    column2: name of the second column in lower case. \n
    table: name of table to query.

    *Response*

    A list of all unique values found between the two columns.
    """
    try:
        result = con.sql(f"SELECT DISTINCT {column1}, {column2} FROM {table} ORDER BY {column2};").fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


@app.get("/get_second_level_class", tags=["Level"], summary="Get second id level class.")
async def get_second_class_list(search_id_first_class:str, con: DuckDBConn = Depends(get_db)):
    """
    Placeholder.

    *Params*

    search_id_first_class: name of the id first class in lower case. \n

    *Response*

    Placeholder.
    """
    try:
        if not search_id_first_class:
            raise HTTPException(status_code=400, detail="Invalid input: search_id_first_class is required")
        
        result = con.sql(f"""
            SELECT DISTINCT {id_second_class}, {second_class_description}
            FROM {table_class}
            WHERE {id_first_class} = ?
            ORDER BY {second_class_description}
        """, params=[search_id_first_class]).fetchall()
        
        return result if result else {"message": "No data found"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/get_third_level_class", tags=["Level"], summary="Get third id level class.")
async def get_third_class_list(search_id_first_class: str, search_id_second_class: str , con: DuckDBConn = Depends(get_db)):
    """
    Placeholder.

    *Params*

    search_id_first_class: name of the id first class in lower case. \n
    search_id_second_class: name of the id secod class in lower case.

    *Response*

    Placeholder.
    """
    try:
        if not search_id_first_class or not search_id_second_class:
            raise HTTPException(status_code=400, detail="Invalid input: id_first_class, id_second_class, and orderedby are required")
        
        result = con.sql(f"""
            SELECT DISTINCT {id_third_class}, {third_class_description}
            FROM {table_class}
            WHERE {id_first_class} = ? AND {id_second_class} = ?
            ORDER BY {third_class_description}
        """, params=[search_id_first_class, search_id_second_class]).fetchall()
        
        return result if result else {"message": "No data found"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/records_by_year", tags=["Record by year"], summary="All records in the year.")
async def get_records_year(year: str, table:str, con: DuckDBConn = Depends(get_db)):
    """
    Get all columns and data in a table, using year as a filter.

    *Params*

    year: numeric value of the year to search. \n
    table: name of table to query.

    *Response*

    A list of all values found in the table that match year.
    """
    try:
        if not year or not table:
            raise HTTPException(status_code=400, detail="Invalid input: year and table are required")
        result = con.sql(f"SELECT * FROM {table} WHERE Anio={year};").fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/calculate_variables", tags=["Covariables"], summary="Calculate population variable data.")
async def calculate_variables(category: str, year : str,
                     cve_enfermedad: str,
                     cve_grupo:str | None = None,
                     cve_causa_def:str | None = None,
                     cve_metropoli : str | None = None,
                     cve_estado : str | None = None, age : str | None = None,
                     gender : str | None = None, con: DuckDBConn = Depends(get_db)):
    """
    Calculate population data using various filters for better geographical precision.

    *Params*

    category: name of the selected category. \n
    year: numerical value of year. \n
    cve_enfermedad: numerical identifier of the disease. \n
    cve_grupo: numerical identifier of the disease group. \n
    cve_causa_def: numerical identifier of death cause. \n
    cve_metropoli: numerical identifier of metropoli zone (this cannot have value if cve_estado is given). \n
    cve_estado: numerical identifier of state (this cannot have value if cve_metropoli is given). \n
    gender: numerical identifier of gender. \n
    age: numerical value of age. \n

    *Response*

    A list of all variables that are in the category.
    """
    try:
        if cve_estado and cve_metropoli:
            raise HTTPException(status_code=400, detail="Invalid parameters: cve_estado and cve_metropoli cannot be in the same request.")

        calc_list = []

        index_distinct_cvegeo = con.sql(f"SELECT DISTINCT indice FROM CATEGORIES WHERE categoria = '{category}' AND anio = {year};").fetchall()
        index_list = [row[0] for row in index_distinct_cvegeo]

        categories_distinct_cvegeo = []
        if cve_estado is not None:
            cvegeo_list = con.sql(f"SELECT DISTINCT cvegeo FROM ESTADO_MUN WHERE cve_estado = {cve_estado};").fetchall()
            for cvegeo in cvegeo_list:
                categories_distinct_cvegeo += con.sql(f"SELECT DISTINCT categoria FROM RAWCOVAR WHERE indice = '{index_list[0]}' AND anio = {year} AND cvegeo = '{cvegeo[0]}'").fetchall()
        elif cve_metropoli is not None:
            if cve_metropoli == "all":
                cvegeo_list = con.sql(f"SELECT DISTINCT cvegeo FROM CVE_METROPOLI WHERE cve_metropoli IS NOT NULL;").fetchall()
            else:
                cvegeo_list = con.sql(f"SELECT DISTINCT cvegeo FROM CVE_METROPOLI WHERE cve_metropoli = '{cve_metropoli}';").fetchall()
            for cvegeo in cvegeo_list:
                categories_distinct_cvegeo += con.sql(f"SELECT DISTINCT categoria FROM RAWCOVAR WHERE indice = '{index_list[0]}' AND anio = {year} AND cvegeo = '{cvegeo[0]}'").fetchall()
        else:
            categories_distinct_cvegeo = con.sql(f"SELECT DISTINCT categoria FROM CATEGORIES WHERE indice = '{index_list[0]}' AND anio = {year}; ").fetchall()
        categories_list = list(set([row[0] for row in categories_distinct_cvegeo]))

        #------N-------
        n_query = f"SELECT SUM(total_population) FROM POPULATION_TOTAL WHERE anio = {year}"
        n = 0
        if cve_estado is not None:
            cvegeo_list = con.sql(f"SELECT DISTINCT cvegeo FROM ESTADO_MUN WHERE cve_estado = {cve_estado};").fetchall()
            for cvegeo in cvegeo_list:
                result = con.sql(f"SELECT SUM(total_population) FROM POPULATION_TOTAL WHERE cvegeo = '{cvegeo[0]}' AND anio = '{year}';").fetchone()
                if result and result[0] is not None:
                    n += result[0]
        elif cve_metropoli is not None:
            if cve_metropoli == "all":
                cvegeo_list = con.sql(f"SELECT DISTINCT cvegeo FROM CVE_METROPOLI WHERE cve_metropoli IS NOT NULL;").fetchall()
            else:
                cvegeo_list = con.sql(f"SELECT DISTINCT cvegeo FROM CVE_METROPOLI WHERE cve_metropoli = '{cve_metropoli}';").fetchall()
            for cvegeo in cvegeo_list:
                result = con.sql(f"SELECT SUM(total_population) FROM POPULATION_TOTAL WHERE cvegeo = '{cvegeo[0]}' AND anio = '{year}';").fetchone()
                if result and result[0] is not None:
                    n += result[0]
        else:
            result = con.sql(n_query).fetchone()
            if result and result[0] is not None:
                n = result[0]

       # ----------NC-----------
        nc_query = "SELECT COUNT(cvegeo) FROM DEFUNCIONES WHERE cve_enfermedad = ? AND anio = ?"
        nc_params = [cve_enfermedad, year]
        if cve_grupo is not None:
            nc_query += " AND cve_grupo = ?"
            nc_params.append(cve_grupo)
        if cve_causa_def is not None:
            nc_query += " AND cve_causa_def = ?"
            nc_params.append(cve_causa_def)
        if cve_estado is not None:
            nc_query += " AND cve_estado = ?"
            nc_params.append(cve_estado)
        if cve_metropoli is not None:
            if cve_metropoli == "all":
                nc_query += " AND cve_metropoli IS NOT NULL"
            else:
                nc_query += " AND cve_metropoli = ?"
                nc_params.append(cve_metropoli)
        if age is not None:
            nc_query += " AND edad_gpo = ?"
            nc_params.append(age)
        if gender is not None:
            nc_query += " AND sexo = ?"
            nc_params.append(gender)

        # Agregar demas filtros
        nc = con.sql(nc_query, params=nc_params).fetchall()[0][0]
        ## ----CATEGORIES--
        for current_category in categories_list:
            query_distinct_cvegeo = con.sql(f"SELECT DISTINCT cvegeo FROM CATEGORIES WHERE categoria = '{current_category}' AND anio = {year};").fetchall()
            cvegeo_list = [str(row[0]) for row in query_distinct_cvegeo]

            # #------NCX----
            ncx_query = "SELECT COUNT(cvegeo) FROM DEFUNCIONES WHERE cve_enfermedad = ? AND anio = ? AND cvegeo = ANY(CAST(? AS VARCHAR[]))"
            ncx_params = [cve_enfermedad, year, cvegeo_list]
            if cve_grupo is not None:
                ncx_query += " AND cve_grupo = ?"
                ncx_params.append(cve_grupo)
            if cve_causa_def is not None:
                ncx_query += " AND cve_causa_def = ?"
                ncx_params.append(cve_causa_def)
            if cve_estado is not None:
                ncx_query += " AND cve_estado = ?"
                ncx_params.append(cve_estado)
            if cve_metropoli is not None:
                if cve_metropoli == "all":
                    nc_query += " AND cve_metropoli IS NOT NULL"
                else:
                    nc_query += " AND cve_metropoli = ?"
                    nc_params.append(cve_metropoli)
            if age is not None:
                ncx_query += " AND edad_gpo = ?"
                ncx_params.append(age)
            if gender is not None:
                ncx_query += " AND sexo = ?"
                ncx_params.append(gender)
            ncx = con.sql(ncx_query, params=ncx_params).fetchone()[0]

            #-----NX-----
            placeholder_cvegeo_list = ""
            for cvegeo in cvegeo_list:
                placeholder_cvegeo_list += "?,"
            nx_query_cve = f"""
                SELECT SUM(total_population) 
                FROM POPULATION_TOTAL 
                WHERE anio = ? 
                AND cvegeo IN ({placeholder_cvegeo_list})
            """
            nx_params = [year] + cvegeo_list
            result = con.sql(nx_query_cve, params=nx_params).fetchone()[0]
            calc_list.append({"category": current_category,"ncx": ncx, "nx":result, "n": n,"nc":nc})

        return calc_list
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/categories", tags=["Covariables"], summary=["Get all categories."])
async def get_categories(year: str, cve_state: str | None = None, cve_metropoli:str | None = None, con: DuckDBConn = Depends(get_db)):
    """
    Get all categories in the project.

    *Params*

    year: numeric value of the year to search. \n

    *Response*

    A list of all categories that match year.
    """
    try:
        if not year:
            raise HTTPException(status_code=400, detail="Invalid input: year is required")
        result = []
        cvegeo_list = []
        if cve_state is not None:
            cvegeo_list = con.sql(f"SELECT DISTINCT cvegeo FROM ESTADO_MUN WHERE cve_estado = {cve_state};").fetchall()
            for cvegeo in cvegeo_list:
                result += con.sql(f"SELECT DISTINCT categoria FROM RAWCOVAR WHERE anio = {year} AND cvegeo='{cvegeo[0]}';").fetchall()
        elif cve_metropoli is not None:
            if cve_metropoli == "all":
                cvegeo_list = con.sql(f"SELECT DISTINCT cvegeo FROM CVE_METROPOLI WHERE cve_metropoli IS NOT NULL;").fetchall()
            else:
                cvegeo_list = con.sql(f"SELECT DISTINCT cvegeo FROM CVE_METROPOLI WHERE cve_metropoli = '{cve_metropoli}';").fetchall()
            for cvegeo in cvegeo_list:
                result += con.sql(f"SELECT DISTINCT categoria FROM RAWCOVAR WHERE anio = {year} AND cvegeo='{cvegeo[0]}';").fetchall()
        else:
            result = con.sql(f"SELECT DISTINCT categoria FROM RAWCOVAR WHERE anio = {year};").fetchall()
        return list(set(result))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/records_by_year_by_column", tags=["Record by year"], summary="Get id first class records in year.")
async def get_records_year_by_coulmn(year: str, search_id_first_class:str, con: DuckDBConn = Depends(get_db)):
    """
    Get record of a column using year as a filter.

    *Params*

    year: numeric value of the year to search. \n
    search_id_first_class: name of id first class to search.

    *Response*

    A list of all values found in the column that match year.
    """
    try:
        if not year or not search_id_first_class:
            raise HTTPException(status_code=400, detail="Invalid input: year and table are required")
        result = con.sql(f"SELECT * FROM {table_records} WHERE Anio={year} AND {id_first_class} = {search_id_first_class};").fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/unique_values_by_column", tags=["Column"], summary="Get unique values of a column.")
async def get_unique_values_by_column(column_name: str, table:str, con: DuckDBConn = Depends(get_db)):
    """
    Get all unique values of a single column in a given table.

    *Params*

    column_name: name of the column in lower case. \n
    table: name of table to query.

    *Response*

    A list of all unique values found in the column.
    """
    try:
        if not column_name or not table:
            raise HTTPException(status_code=400, detail="Invalid input: column_name and table are required")
        result = con.sql(f"SELECT DISTINCT {column_name} FROM {table} ORDER BY {column_name}").fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/get_all_by_table", tags=["Show"], summary="Show all data of table.")
async def get_all_the_values(table:str, con: DuckDBConn = Depends(get_db)):
    """
    Get all columns and data of a given table (takes time to complete).

    *Params*

    table: name of table to query.

    *Response*

    A list of all the data in a table.
    """
    try:
        if not table:
            raise HTTPException(status_code=400, detail="Invalid input: table required")
        result = con.sql(f"SELECT * FROM {table}").fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/get_population", tags=["Population"], summary="Get all population")
async def get_population(year: str, cvegeo:str, edad_gpo:str = "", sexo:str = "", con: DuckDBConn = Depends(get_db)):
    """
    Get all population that match the filters.

    *Params*

    year: numerical value of year. \n
    cvegeo: numerical value of cvegeo. \n
    edad_gpo: range of age values (ex. 0-04). \n
    sexo: Gender value can be HOMBRES or MUJERES. \n

    *Response*

    A list of all population that match the query.
    """
    try:
        if not year and not cvegeo:
            raise HTTPException(status_code=400, detail="Invalid input: year and cvegeo are required")
        params = [cvegeo, year]
        if edad_gpo == "" and sexo == "":
            query = "SELECT sexo, edad_gpo, poblacion FROM POPULATION WHERE cvegeo = ? AND anio = ?"
        elif edad_gpo != "" and sexo == "":
            query = "SELECT edad_gpo, SUM(poblacion) FROM POPULATION_AGE WHERE cvegeo = ? AND anio = ?" \
            "AND edad_gpo = ? GROUP BY edad_gpo"
            params.append(edad_gpo)
        elif edad_gpo == "" and sexo != "":
            query = "SELECT sexo, SUM(poblacion) FROM POPULATION_GENDER WHERE cvegeo = ? AND anio = ?" \
            "AND sexo = ? GROUP BY sexo"
            params.append(sexo)
        else:
            query = "SELECT poblacion FROM POPULATION WHERE cvegeo = ? AND anio = ? AND edad_gpo = ? AND sexo = ?"
            params.append(edad_gpo)
            params.append(sexo)
        result = con.sql(query, params=params).fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

def delete_tmp_file(path: str) -> None:
    os.unlink(path)
    

@app.get("/variables/id", tags=["Project variables"], summary="Get all variables of id")
async def get_variables_id(id: str, con: DuckDBConn = Depends(get_db)):
    """
    Get all variables of a given id.

    *Params*

    id: name of the id. \n

    *Response*

    A JSON of all variables that match id value.
    """
    try:
        var_table = ""
        cve = ""
        atributo = ""
        atr_id = []
        year_to_search = 0
        if not id or id == "":
            raise HTTPException(status_code=400, detail="Invalid input: id is required")
        if id.startswith("EN"):
            var_table = "DATA_VAR_DISEASES"
            cve = "cve_enfermedad"
            atributo = "enfermedad"
            atr_id = con.sql(f"""SELECT {cve}, {atributo} FROM VAR_DISEASES WHERE VAR_DISEASES.id = '{id}'""").fetchone()
        elif id.startswith("GR"):
            var_table = "DATA_VAR_GROUP"
            cve = "cve_grupo"
            atributo = "grupo"
            atr_id = con.sql(f"""SELECT {cve}, {atributo} FROM VAR_GROUP WHERE VAR_GROUP.id = '{id}'""").fetchone()
        else:
            check_cause = con.sql(f"""SELECT DISTINCT id FROM VAR_DISEASES""").fetchall()
            if check_cause.count(id) <= 0:
                raise HTTPException(status_code=400, detail="Invalid input: id not found")
            var_table = "DATA_VAR_CAUSEDEATH"
            cve = "cve_causa_def"
            atributo = "causa_def"
            atr_id = con.sql(f"""SELECT {cve}, {atributo} FROM VAR_CAUSEDEATH WHERE VAR_CAUSEDEATH.id = '{id}'""").fetchone()
        year_to_search = int("20" + str(id[-2:]))
    
        search_count = con.sql(f"""
                SELECT anio, cvegeo, count FROM {var_table}
                WHERE {var_table}.id = '{id}'
                AND anio = {year_to_search}
                GROUP BY anio, cvegeo, count;""").df()
        
        search_count['decile'] = qcut(search_count['count'].rank(method='first'), 10, labels=range(1,11))
        decile_summary = search_count.groupby('decile').agg(municipalities=('count', 'size'), min_count=('count', 'min'), max_count=('count', 'max') ).reset_index() 
        print(decile_summary)
        
        result = []
        num_val = 1
        for val_min, val_max in zip(decile_summary["min_count"], decile_summary["max_count"]):
            result += con.sql(f"""SELECT DISTINCT CONCAT(
                         '{{"id": "{id}",
                         "level_id": "{id}-{num_val}",
                         "bin": {num_val}, "data": ["{atr_id[0]}", "{atr_id[1]}", "({val_min}-{val_max}]"]}}')
                         FROM {var_table}
                         ORDER BY id""").fetchall()
            num_val += 1

        parsed_result = []
        for row in result:
            try:
                parsed_result.append(json.loads(row[0]))
            except json.JSONDecodeError as e:
                print(f"Invalid JSON in row: {row[0]}, Error: {e}")
                raise HTTPException(status_code=500, detail=f"Invalid JSON in row: {row[0]}")
        return jsonable_encoder(parsed_result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/variables", tags=["Project variables"], summary="Get variables data in project")
async def get_variables(con: DuckDBConn = Depends(get_db)):
    """
    Get all variables data in the project.

    *Response*

    A JSON of all variables data.
    """
    try:
        result = con.sql(f"""
            SELECT DISTINCT CONCAT(
                '{{"id": "', id, 
                '", "name": "', REGEXP_REPLACE(enfermedad, '"', '\\"'), 
                '", "level_size": 10, "filter_fields": [], "available_grids": ["mun"]}}'
            )
            FROM VAR_DISEASES 
            ORDER BY id
        """).fetchall()

        result += con.sql(f"""
            SELECT DISTINCT CONCAT(
                '{{"id": "', id, 
                '", "name": "', REGEXP_REPLACE(grupo, '"', '\\"'), 
                '", "level_size": 10, "filter_fields": [], "available_grids": ["mun"]}}'
            )
            FROM VAR_GROUP 
            ORDER BY id
        """).fetchall()
        
        result += con.sql(f"""
            SELECT DISTINCT CONCAT(
                '{{"id": "', id, 
                '", "name": "', REGEXP_REPLACE(causa_def, '"', '\\"'), 
                '", "level_size": 10, "filter_fields": [], "available_grids": ["mun"]}}'
            )
            FROM VAR_CAUSEDEATH
            ORDER BY id
        """).fetchall()

        parsed_result = []
        for row in result:
            try:
                parsed_result.append(json.loads(row[0]))
            except json.JSONDecodeError as e:
                print(f"Invalid JSON in row: {row[0]}, Error: {e}")
                raise HTTPException(status_code=500, detail=f"Invalid JSON in row: {row[0]}")
        return jsonable_encoder(parsed_result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")
 

# EndPoint for cleaning a csv file and enable a download- 
@app.post("/upload_csv", tags=["Upload CSV file"], summary="Clean a user uploaded csv")
async def upload_csv(file: UploadFile = File(...)):
    """
    Receive a CSV file, then cleans encoding errors and enables a download link of the same file in a UTF-8 encoding.

    *Params*

    file: CSV file to clean and convert. \n

    *Response*

    A download link of the uploaded file in UTF-8 encoding.
    """
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only csv files allowed")
    try:
        fd, path = tempfile.mkstemp(suffix=".csv", text=True)
        download_file_name = "cleaned_" + file.filename
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            clean_csv_in_chunks(input_path=file.file, output_path=f)
        return FileResponse(path, media_type="text/csv", filename=download_file_name,
                            background=BackgroundTasks().add_task(delete_tmp_file, path))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
