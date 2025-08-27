from fastapi import Depends, FastAPI, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.background import BackgroundTasks
from contextlib import contextmanager
from typing import Generator
from fastapi.encoders import jsonable_encoder
from services.clean_csv import clean_csv_in_chunks, db_columns_to_lowercase
from services.file_helper_functions import get_csv_in_directory_to_clean
import os
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv
from io import StringIO
import tempfile
import json

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
            ######################

            db_connection.sql("""
                CREATE OR REPLACE TABLE ENFERMEDADES AS
                SELECT DISTINCT CVE_Grupo, Grupo, CVE_Enfermedad, Enfermedad, CVE_Causa_def, Causa_def
                FROM RAWDATA;
            """)
            db_columns_to_lowercase("ENFERMEDADES", db_connection)

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

@app.get("/get_variables")
async def get_variables(con: DuckDBConn = Depends(get_db)):
    try:
        result = con.sql(f"SELECT DISTINCT CONCAT(" \
                         "'ID: ', cve_enfermedad, ', name: ', enfermedad, ', level_size: 3, filter_fields: [“anio”, “sexo”, “edad”, “muncipio”], available_grids: [18, 19]')" \
                         " FROM ENFERMEDADES ORDER BY cve_enfermedad;").fetchall()
        
        result += con.sql(f"SELECT DISTINCT CONCAT(" \
                         "'ID: ', cve_grupo, ', name: ', grupo, ', level_size: 2, filter_fields: [“anio”, “sexo”, “edad”, “muncipio”], available_grids: [18, 19]')" \
                         " FROM ENFERMEDADES ORDER BY cve_grupo;").fetchall()
        
        result += con.sql(f"SELECT DISTINCT CONCAT(" \
                         "'ID: ', cve_causa_def, ', name: ', causa_def, ', level_size: 1, filter_fields: [“anio”, “sexo”, “edad”, “muncipio”], available_grids: [18, 19]')" \
                         " FROM ENFERMEDADES ORDER BY cve_causa_def;").fetchall()
        return json.dumps(result, ensure_ascii=False, indent= 1)
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
