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
                SELECT DISTINCT CVE_Metropoli, Metropolis, CAST(CVEGEO AS VARCHAR) AS CVEGEO
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
async def covar_test(categoria: str, anio : str, 
                     cve_enfermedad: str,
                     cve_grupo:str | None = None,
                     cve_causa_def:str | None = None,
                     cve_metropoli : str | None = None,
                     cve_estado : str | None = None, edad : str | None = None,
                     genero : str | None = None, con: DuckDBConn = Depends(get_db)):
    try:
        if cve_estado and cve_metropoli:
            raise HTTPException(status_code=400, detail="Invalid parameters: cve_estado and cve_metropoli cannot be in the same request.")

        calc_list = []
        helper_cveo_list = []

        index_distinct_cvegeo = con.sql(f"SELECT DISTINCT indice FROM RAWCOVAR WHERE categoria = '{categoria}' AND anio = {anio};").fetchall()
        index_list = [row[0] for row in index_distinct_cvegeo]
        print("🍕🥮🥮🥮🥮🥮")
        print(index_list)
        print(len(index_list))
        print("🍕🥮🥮🥮🥮🥮")
        index_distinct_cvegeo1 = con.sql(f"SELECT DISTINCT cvegeo FROM RAWCOVAR WHERE anio = {anio} AND indice = '{index_list[0]}';").fetchall()
        index_list1 = [row[0] for row in index_distinct_cvegeo1]
        print(index_list1)
        #-------2469 mun--------
        print(len(index_list1))
        print("🍕🥮🥮🥮🥮🥮")

 

        categories_distinct_cvegeo = con.sql(f"SELECT DISTINCT categoria FROM RAWCOVAR WHERE indice = '{index_list[0]}' AND anio = {anio}; ").fetchall()
        categories_list = [row[0] for row in categories_distinct_cvegeo]
        print("🍕xxxxxx")
        print(len(categories_list))
        print("🍕xxxx")
        
        for cat in categories_list:
            categories_distinct_cvegeo = con.sql(f"SELECT DISTINCT cvegeo FROM RAWCOVAR WHERE categoria = '{cat}' AND anio = {anio}; ").fetchall()
            muns = [row[0] for row in categories_distinct_cvegeo]

        print("🍕MUNS")



        test = con.sql(f"SELECT DISTINCT * FROM RAWCOVAR WHERE anio = {anio} AND cvegeo=21021").fetchall()
        # test1 = con.sql(f"SELECT DISTINCT * FROM RAWCOVAR WHERE anio = {anio}").fetchall()
        v = [row[0] for row in test]
        print("🎏")
        print("🎏")
        print("🎏")
        print(test)
        print(v)
        print("🎏")
        print("🎏")
        print("🎏")
        print("🎏")

        mun_total = con.sql(f"SELECT DISTINCT cvegeo FROM POPULATION_TOTAL WHERE anio = {anio};").fetchall()
        a = [row[0] for row in mun_total]


        #------N-------
        n_query = f"SELECT SUM(total_population) FROM POPULATION_TOTAL WHERE anio = {anio}"
        params = [anio]
        n = 0
        if cve_estado is not None:
            cvegeo_list = con.sql(f"SELECT DISTINCT cvegeo FROM ESTADO_MUN WHERE cve_estado = {cve_estado};").fetchall()
            print(len(cvegeo_list))
            print(cvegeo_list)
            for cvegeo in cvegeo_list:
                result = con.sql(f"SELECT SUM(total_population) FROM POPULATION_TOTAL WHERE cvegeo = '{cvegeo[0]}' AND anio = '{anio}';").fetchone()
                print(result)
                if result and result[0] is not None:
                    n += result[0]
        elif cve_metropoli is not None:
            cvegeo_list = con.sql(f"SELECT DISTINCT cvegeo FROM METROPOLI WHERE cve_metropoli = '{cve_metropoli}';").fetchall()
            print(len(cvegeo_list))
            print(cvegeo_list)
            for cvegeo in cvegeo_list:
                result = con.sql(f"SELECT SUM(total_population) FROM POPULATION_TOTAL WHERE cvegeo = '{cvegeo[0]}' AND anio = '{anio}';").fetchone()
                print(result)
                if result and result[0] is not None:
                    n += result[0]
        else:
            result = con.sql(n_query).fetchone()
            if result and result[0] is not None:
                n = result[0]

       # ----------NC-----------
        nc_query = "SELECT COUNT(cvegeo) FROM DEFUNCIONES WHERE cve_enfermedad = ? AND anio = ?"
        params = [cve_enfermedad, anio]
        if cve_grupo is not None:
            nc_query += " AND cve_grupo = ?"
            params.append(cve_grupo)
        if cve_causa_def is not None:
            nc_query += " AND cve_causa_def = ?"
            params.append(cve_causa_def)
        if cve_estado is not None: ## SI FUNCIONA
            nc_query += " AND cve_estado = ?"
            params.append(cve_estado)
        if cve_metropoli is not None: #POR PROBAR
            nc_query += " AND cve_metropoli = ?"
            params.append(cve_metropoli)
        nc = con.sql(nc_query, params=params).fetchall()

        ## ----CATEGORIES--

        # for cat in categories_list:
        #     categories_distinct_cvegeo = con.sql(f"SELECT DISTINCT cvegeo FROM RAWCOVAR WHERE categoria = '{cat}' AND anio = {anio}; ").fetchall()
        #     muns = [row[0] for row in categories_distinct_cvegeo]

        cvetest = []
        xxx = 0
        helper = []
        for category in categories_list:
            # --------lista de cvegeo de la categoria por año--------
            query_distinct_cvegeo = con.sql(f"SELECT DISTINCT cvegeo FROM RAWCOVAR WHERE categoria = '{category}' AND anio = {anio};").fetchall()
            cvegeo_list = [str(row[0]) for row in query_distinct_cvegeo]
            cvetest += cvegeo_list
            print("LISTAAAAA")
            print(len(cvetest))
            print("LISTAAAAA")

            # #------NCX----
            ncx_query = "SELECT COUNT(cvegeo) FROM DEFUNCIONES WHERE cve_enfermedad = ? AND anio = ? AND cvegeo = ANY(CAST(? AS VARCHAR[]))"
            params = [cve_enfermedad, anio, cvegeo_list]
            
            if cve_grupo is not None:
                ncx_query += " AND cve_grupo = ?"
                params.append(cve_grupo)
            if cve_causa_def is not None:
                ncx_query += " AND cve_causa_def = ?"
                params.append(cve_causa_def)
            if cve_estado is not None:
                ncx_query += " AND cve_estado = ?"
                params.append(cve_estado)
            if cve_metropoli is not None:
                ncx_query += " AND cve_metropoli = ?"
                params.append(cve_metropoli)
            if edad is not None:
                ncx_query += " AND edad_gpo = ?"
                params.append(edad)
            if genero is not None:
                ncx_query += " AND sexo = ?"
                params.append(genero)
            
            ncx = con.sql(ncx_query, params=params).fetchone()[0]
            
            print("😱")
            print(ncx)
            xxx = ncx
        

            #-----NX-----
            ## RAWCOVAR
            ## DEFUNCIONES
            # son las defunciones que tienen la variable
            
            # #
            placeholders_cvegeo_list = ""
            for cve in cvegeo_list:
                placeholders_cvegeo_list += "?,"
            nx_query_cve = f"""
                SELECT SUM(total_population) 
                FROM POPULATION_TOTAL 
                WHERE anio = ? 
                AND cvegeo IN ({placeholders_cvegeo_list})
            """
            nx_params = [anio] + cvegeo_list
            print(nx_query_cve)
            print("🌈")
            
            # Execute the query
            result = con.sql(nx_query_cve, params=nx_params).fetchone()[0]
            print(result)
            print("🎯 Total population:")
            helper.append({"category": category,"ncx": ncx, "nx":result})
            # result2 += con.sql(f"""
            #         SELECT SUM(total_population) 
            #         FROM POPULATION_TOTAL 
            #         WHERE anio = 2000 
            #         AND cvegeo = {cve}
            #         GROUP BY anio
            #     """).fetchone()[0]
            # #
            # # print(nx)
            # name = con.sql(f"SELECT DISTINCT cvegeo FROM RAWCOVAR WHERE categoria = '{categoria}' AND anio = {anio};").fetchall()
            # cvegeos = [str(row[0]) for row in name]
            # # nx_query = "SELECT DISTINCT cvegeo FROM DEFUNCIONES WHERE cve_enfermedad = ? AND anio = ? AND cvegeo IN (" + ",".join(["?"] * len(cvegeo_list)) + ")"
            # deaths_query = """
            #     SELECT DISTINCT cvegeo 
            #     FROM DEFUNCIONES 
            #     WHERE cve_enfermedad = ? 
            #     AND anio = ? 
            #     AND cvegeo IN ({})
            #     """.format(",".join(["?"] * len(cvegeos)))
            #
            # print(deaths_query)
            # params = [cve_enfermedad, anio] + cvegeos
            #
            # if cve_grupo is not None:
            #     deaths_query += " AND cve_grupo = ?"
            #     params.append(cve_grupo)
            #
            # if cve_causa_def is not None:
            #     deaths_query += " AND cve_causa_def = ?"
            #     params.append(cve_causa_def)
            # # agregar esta validacíon al principio y eliminar la repetida
            # if cve_estado and cve_metropoli:
            #     raise HTTPException(status_code=400, detail="Invalid parameters: cve_estado and cve_metropoli cannot be in the same request.")
            #
            # if cve_estado is not None:
            #     deaths_query += " AND cve_estado = ?"
            #     params.append(cve_estado)
            #
            # if cve_metropoli is not None:
            #     deaths_query += " AND cve_metropoli = ?"
            #     params.append(cve_metropoli)
            #
            # if edad is not None:
            #     deaths_query += " AND edad_gpo = ?"
            #     params.append(edad)
            #
            # if genero is not None:
            #     deaths_query += " AND sexo = ?"
            #     params.append(genero)
            #
            # # Execute the query
            #
            # df = con.sql(deaths_query, params=params).df()
            # cvegeos_filtered = df['cvegeo'].astype(str).tolist()
            #
            # print("<<😱>>")
            # print(cvegeos_filtered)
            # print(len(cvegeos_filtered))
            # print("-😱-")
            # print("-😱-")
            # print("-😱-")
            # print("-😱-")
            # print("-😱-")
            # print("-😱-")
            #
            # query = "SELECT SUM(total_population) FROM POPULATION_TOTAL WHERE anio = ? AND cvegeo IN (" + ",".join(["?"] * len(cvegeo_filtered_list)) + ")"
            #nx_query = """
            #     SELECT SUM(total_population) 
            #     FROM POPULATION_TOTAL 
            #     WHERE anio = ? 
            #     AND cvegeo IN ({})
            # """.format(",".join(["?"] * len(cvegeo_filtered_list)))
            # nx_query_test = """
            #     SELECT cvegeo, total_population
            #     FROM POPULATION_TOTAL 
            #     WHERE anio = ? 
            #     AND cvegeo IN ({})
            # """.format(",".join(["?"] * len(cvegeo_filtered_list)))
            
            # print(nx_query)
            # print("🌈>>>>>>")
            # print(nx_query_test)
            # print("🌈>>>>>>")
            
            # Build the params list
            # nx_params = [anio] + cvegeo_filtered_list
            # print("Population query:", nx_query)
            # print(f"First param (anio): {nx_params[0]} (type: {type(nx_params[0])})")
            # print(f"Number of params: {len(nx_params)}")
            # print(nx_params)
            
            # Execute the query
            # result = con.sql(nx_query, params=nx_params).fetchall()
            # result_test = con.sql(nx_query_test, params=nx_params).fetchall()
            # print(f"Result: {result}")
            # print(f"Result🫧:: {result_test}")
            # if set(cvegeo_filtered_list) == set(cvegeos_filtered):
            #     print("Same values")
            # else:
            #     print("Different values")        
            # print("🎯 Total population:", result)
            # calc_list.append({"category":category, "n": n, "nc":nc[0][0], "ncx":ncx, "nx": result[0][0]})

        print("-----xxx----")
        print(xxx)
        print(helper)
        print("xxx")
        print(len(cvetest))
        # print(list(set(index_list1) - set(cvetest)))
        print("^^^^^^^^^^^")
        # print(helper_cveo_list)
        # print(len(helper_cveo_list))
        # unique = list(set(helper_cveo_list))
        # print(unique)
        # print(len(unique))
        # difference = list(set(a) - set(unique))
        # print("🏮") 
        # print(difference) 
        # print(len(difference)) 
        # print("🏮") 
        return calc_list
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
