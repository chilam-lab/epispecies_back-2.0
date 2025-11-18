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

app = FastAPI()

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
                CREATE OR REPLACE TABLE RAWCOVAR AS
                SELECT * FROM 'duckdb_files/RAWCOVAR.parquet';
            """)
            db_columns_to_lowercase("RAWCOVAR", db_connection)


            
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
                CREATE OR REPLACE TABLE RAW_CVE_METROPOLIS AS
                SELECT * FROM 'duckdb_files/RAW_CVE_METROPOLIS.parquet';
            """)
            db_columns_to_lowercase("RAW_CVE_METROPOLIS", db_connection)

            db_connection.sql("""
                CREATE OR REPLACE TABLE CVE_METROPOLIS AS
                SELECT DISTINCT CAST(CVEGEO AS VARCHAR) AS CVEGEO, CVE_SUN, Metropoli
                FROM RAW_CVE_METROPOLIS;
            """)
            db_columns_to_lowercase("CVE_METROPOLIS", db_connection)

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

            db_connection.sql("""
                CREATE OR REPLACE TABLE METROPOLI AS
                SELECT DISTINCT CVE_Metropoli, Metropolis
                FROM RAWDATA;
            """)
            db_columns_to_lowercase("METROPOLI", db_connection)
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

@app.get("/show/tables")
async def show_tables(con: DuckDBConn = Depends(get_db)):
    try:
        tables = con.sql("SHOW TABLES")
        result = tables.to_df().to_dict(orient="records")
        return {"tables": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/show/columns")
async def get_columns(table_name:str, con: DuckDBConn = Depends(get_db)):
    try:
        rel = con.sql(f"DESCRIBE {table_name}")
        column_names = [row[0] for row in rel.fetchall()]
        return jsonable_encoder({"columns": column_names})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/unique_pair_columns")
async def get_unique_pair_columns(column1: str, column2: str, table: str, con: DuckDBConn = Depends(get_db)):
    try:
        result = con.sql(f"SELECT DISTINCT {column1}, {column2} FROM {table} ORDER BY {column2};").fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


@app.get("/get_second_level_class")
async def get_second_class_list(search_id_first_class:str, con: DuckDBConn = Depends(get_db)):
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

@app.get("/get_third_level_class")
async def get_third_class_list(search_id_first_class: str, search_id_second_class: str , con: DuckDBConn = Depends(get_db)):
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

@app.get("/records_by_year")
async def get_records_year(year: str, table:str, con: DuckDBConn = Depends(get_db)):
    try:
        if not year or not table:
            raise HTTPException(status_code=400, detail="Invalid input: year and table are required")
        result = con.sql(f"SELECT * FROM {table} WHERE Anio={year};").fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/covar_test")
async def covar_test(categoria: str, anio : str, enfermedades: list[str] = Query(..., min_length=1, max_length=3),
                     cve_estado : str | None = None, edad : str | None = None,
                     genero : str | None = None, con: DuckDBConn = Depends(get_db)):
    try:
        has_cve_estado = False
        has_edad = False
        has_genero = False
        st = round(time.time() * 1000)

        alldeaths = []
        filterdeaths = []
        pob_total = []
        query_params = ""
        death_by_disease_1 = []
        death_by_disease_2 = []
        death_by_disease_3 = []
    
        #Add to query the optional parameters, if exists
        n = 0
        list_mun = []
        if cve_estado:
            has_cve_estado = True
            query_params = query_params + f" AND cve_estado = {cve_estado}"
        if edad:
            has_edad = True
            query_params = query_params + f" AND edad = {edad}"
        if genero:
            has_genero = True
            query_params = query_params + f" AND genero = '{genero}'"


        #Variables for n
        q_estado = []
        pob_n = []
        query_for_pop_total = []
        n = 0
            

        #Variables for nx
        query_nx = []
        nx = 0
        
        #First, we search in RAWCOVAR table for the cvegeo value of the category
        query_distint_cvegeo = con.sql(f"SELECT cvegeo FROM RAWCOVAR WHERE categoria= '{categoria}' AND anio = {anio};").fetchall()

        #Then, we search deaths in the DEFUNCIONES table
        for cvegeo in query_distint_cvegeo:
            alldeaths += con.sql(f"SELECT DISTINCT cvegeo FROM DEFUNCIONES WHERE cvegeo = {cvegeo[0]} AND anio={anio};").fetchall()
        
        #We filter by diseases
        disease_num = 0
        for disease in enfermedades:
            disease_num += 1
            for cvegeo in alldeaths:
                cve_disease = con.sql(f"SELECT DISTINCT cve_enfermedad FROM ENFERMEDADES WHERE enfermedad='{disease}';").fetchone()
                if disease_num == 1:
                    death_by_disease_1 += con.sql(f"SELECT DISTINCT cvegeo, cve_enfermedad FROM DEFUNCIONES WHERE cvegeo = {cvegeo[0]} AND anio={anio} AND cve_enfermedad={cve_disease[0]};").fetchall()
                elif disease_num == 2:
                    death_by_disease_2 += con.sql(f"SELECT DISTINCT cvegeo, cve_enfermedad FROM DEFUNCIONES WHERE cvegeo = {cvegeo[0]} AND anio={anio} AND cve_enfermedad={cve_disease[0]};").fetchall()
                else:
                    death_by_disease_3 += con.sql(f"SELECT DISTINCT cvegeo, cve_enfermedad FROM DEFUNCIONES WHERE cvegeo = {cvegeo[0]} AND anio={anio} AND cve_enfermedad={cve_disease[0]};").fetchall()
        filterdeaths = death_by_disease_1 + death_by_disease_2 + death_by_disease_3
        
        #Next we search in the POPULATION table (TO DO)

        # for cvegeo in def_cvegeo:
        #     if query_params == "":
        #         pob_total += con.sql(f"SELECT DISTINCT anio, cvegeo, total_population FROM POPULATION_TOTAL WHERE cvegeo = {cvegeo[0]} AND anio={anio};").fetchall()
        #         continue

        #     if has_cve_estado:
        #         list_mun += con.sql(f"SELECT DISTINCT cvegeo, cve_estado FROM ESTADO_MUN WHERE cve_estado= {cve_estado} AND cvegeo = {cvegeo[0]};").fetchall()

        #     if has_edad and not has_genero: #Only edad 
        #         query_for_pop_total += con.sql(f"SELECT DISTINCT cvegeo, anio, edad_gpo, poblacion FROM POPULATION_AGE WHERE cvegeo = {cvegeo[0]} AND anio={anio} AND edad_gpo = '{edad}';").fetchall()
        #     elif has_genero and not has_edad: #Only genero 
        #         query_for_pop_total += con.sql(f"SELECT DISTINCT cvegeo, anio, sexo, poblacion FROM POPULATION_GENDER WHERE cvegeo = {cvegeo[0]} AND anio={anio} AND sexo = '{genero}';").fetchall()
        #     else: #Both
        #         query_for_pop_total += con.sql(f"SELECT DISTINCT cvegeo, anio, sexo, poblacion, edad_gpo FROM POPULATION WHERE cvegeo = {cvegeo[0]} AND anio={anio} AND sexo = '{genero}' AND edad_gpo = '{edad}';").fetchall()
        #     #result += con.sql(f"SELECT DISTINCT * FROM DEFUNCIONES WHERE cvegeo = {res[0]} AND anio={anio};").fetchall()




        #     #result += con.sql(f"SELECT DISTINCT * FROM DEFUNCIONES WHERE cvegeo = {res[0]} AND anio={anio};").fetchall()
        #     #Adding data to query_nx, using this query for nx value
        #     #query_nx += con.sql(f"SELECT cvegeo, anio, poblacion FROM POPULATION WHERE cvegeo = {res[0]} AND anio={anio};").fetchall()
        #     #Adding data to q_estado, using this query for n value
        #     #if has_cve_estado:
        #     #    q_estado += con.sql(f"SELECT cvegeo, cve_estado FROM ESTADO_MUN WHERE cve_estado= {cve_estado} AND cvegeo = {res[0]};").fetchall()
        #     #else:
        #     #    q_estado += con.sql(f"SELECT cvegeo FROM ESTADO_MUN WHERE cvegeo = {res[0]};").fetchall()
        
        # if len(list_mun) != 0 and len(query_for_pop_total) == 0:
        #     for cvegeo in list_mun:
        #         pob_total += con.sql(f"SELECT DISTINCT anio, cvegeo, total_population FROM POPULATION_TOTAL WHERE cvegeo = {cvegeo[0]} AND anio={anio};").fetchall()
    
        # if len(query_for_pop_total) != 0:
        #     for cvegeo in query_for_pop_total:
        #         if has_cve_estado and has_edad and has_genero:
        #             pob_total += con.sql(f"SELECT DISTINCT cvegeo, anio, poblacion, sexo, edad_gpo FROM POPULATION WHERE cvegeo = {cvegeo[0]} AND anio={anio} AND sexo = '{genero}' AND edad_gpo = '{edad}'").fetchall()
        #         elif not has_cve_estado:
        #             n += cvegeo[3]

        # for pop_total_cvegeo in pob_total:
        #     n += pop_total_cvegeo[2]

        # #getting the values of all population using q_estado query, then using sum to obtain the value for n
        # #for pob in q_estado: 
        # #    pob_n += con.sql(f"SELECT cvegeo, poblacion FROM POPULATION WHERE cvegeo = {pob[0]};").fetchall()
        # #for pob_list in pob_n:
        # #    n += sum(i for i in pob_list if isinstance(i, int))
        
        # #getting the value of nx, using query_nx third value
        # #for nx_list in query_nx:
        # #    nx += nx_list[2]
        
        # #getting the value of ncx, using the rows of the result query
        # #ncx = len(result)


        end = round(time.time() * 1000)
        print("Miliss to finish: " + str(end - st))
        #union_val_test = [("poblacion total (n): ", n), ("poblacion que vive en la entidad (nx): ", nx), ("No. de casos (ncx): ", ncx)]
        return filterdeaths
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/records_by_year_by_column")
async def get_records_year_by_coulmn(year: str, search_id_first_class:str, con: DuckDBConn = Depends(get_db)):
    try:
        if not year or not search_id_first_class:
            raise HTTPException(status_code=400, detail="Invalid input: year and table are required")
        result = con.sql(f"SELECT * FROM {table_records} WHERE Anio={year} AND {id_first_class} = {search_id_first_class};").fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@app.get("/unique_values_by_column")
async def get_unique_values_by_column(column_name: str, table:str, con: DuckDBConn = Depends(get_db)):
    try:
        if not column_name or not table:
            raise HTTPException(status_code=400, detail="Invalid input: column_name and table are required")
        result = con.sql(f"SELECT DISTINCT {column_name} FROM {table} ORDER BY {column_name}").fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/get_all_by_table")
async def get_all_the_values(table:str, con: DuckDBConn = Depends(get_db)):
    try:
        if not table:
            raise HTTPException(status_code=400, detail="Invalid input: column_name and table are required")
        result = con.sql(f"SELECT * FROM {table}").fetchall()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/get_population")
async def get_population(year: str, cvegeo:str, edad_gpo:str = "", sexo:str = "", con: DuckDBConn = Depends(get_db)):
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
    

@app.get("/variables/id")
async def get_variables_id(id: str, con: DuckDBConn = Depends(get_db)):
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

@app.get("/variables")
async def get_variables(con: DuckDBConn = Depends(get_db)):
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
@app.post("/upload_csv")
async def upload_csv(file: UploadFile = File(...)):
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