from fastapi import Depends, FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from contextlib import contextmanager
from typing import Generator
from fastapi.encoders import jsonable_encoder
from services.clean_csv import clean_csv_in_chunks, create_csv_from_cleaned
import os
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv
import csv
from io import StringIO

from os import listdir, remove
from os.path import isfile, join, splitext, exists
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
    con = duckdb.connect("db/my_database.db")
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
        if not os.path.exists('db/cleaned_file.csv'):
            clean_csv_in_chunks('db/def00_19_v2.csv', 'db/cleaned_file.csv')
        db_connection.sql("""
            COPY (SELECT * FROM read_csv_auto('db/cleaned_file.csv', auto_detect=true, header=true))
            TO 'db/RAWDATA.parquet' (FORMAT PARQUET);
        """)
        db_connection.sql("""
            CREATE OR REPLACE TABLE RAWDATA AS
            SELECT * FROM 'db/RAWDATA.parquet';
        """)
        db_connection.sql("""
            CREATE OR REPLACE TABLE ENFERMEDADES AS
            SELECT DISTINCT CVE_Grupo, Grupo, CVE_Enfermedad, Enfermedad, CVE_Causa_def, Causa_def
            FROM RAWDATA;
        """)
        db_connection.sql("CREATE INDEX IF NOT EXISTS id_enfermedad ON ENFERMEDADES (CVE_Enfermedad, CVE_Grupo, CVE_Causa_def);")
        db_connection.sql("""
            CREATE OR REPLACE TABLE DEFUNCIONES AS
            SELECT CVE_Enfermedad, CVE_Grupo, CVE_Causa_def, CVE_Estado,
            CVEGEO, CVE_Metropoli, Ambito, Sexo, Edad_gpo, Ocupacion, Escolaridad, Edo_civil, Anio
            FROM RAWDATA;
        """)
        db_connection.sql("""
            CREATE OR REPLACE TABLE ESTADO_MUN AS
            SELECT DISTINCT CVE_Estado, Estado, CVEGEO, Municipio
            FROM RAWDATA;
        """)
        db_connection.sql("""
            CREATE OR REPLACE TABLE METROPOLI AS
            SELECT DISTINCT CVE_Metropoli, Metropolis
            FROM RAWDATA;
        """)
    except Exception as e:
        print(f"Error creating table: {e}")
        tables = db_connection.sql("SHOW TABLES").fetchall()
        print("Available tables:", [row[0] for row in tables])

@app.on_event("startup")
async def startup_event():
    global db_connection
    db_connection = duckdb.connect("app/db/my_database.db")
    #init_db()

@app.on_event("shutdown")
async def shutdown_event():
    global db_connection
    if db_connection:
        db_connection.close()

@app.get("/")
async def root(con: DuckDBConn = Depends(get_db)):
    return {"message": "Hello World"}

@app.get("/create")
async def create_table(con: DuckDBConn = Depends(get_db)):
    try:
     #   first_chunk = True
      #  db_dir = "db"
       # name_of_cleaned_file = ""
       # dir_clean_csv = "cleanedCSV/"
       # csv_cleaned_name = join(db_dir, name_of_cleaned_file)
        #for file in listdir("db/"):
            #First, we check for .csv files that area not cleaned
         #   if file.endswith(".csv") and not file.startswith("cleaned_"):
          #      csv_file_dir = join(db_dir, file)
           #     name_of_cleaned_file = join('db/cleaned_', file)
                #If the file cleaned_file does not exist, we clean and create it
           #     if isfile((join(dir_clean_csv, 'clean_table_file-csv'))):
            #        clean_csv_in_chunks(first_chunk, csv_file_dir, csv_cleaned_name)
                #We create the table
                con.sql(f"""
                        CREATE OR REPLACE TABLE deaths AS
                        SELECT * FROM read_csv_auto('cleanedCSV/csv_to_table_file.csv',
                        auto_detect=true, header=true);""")
                con.sql(f"""
                        COPY (SELECT * FROM read_csv_auto('cleanedCSV/csv_to_table_file.csv', auto_detect=true, header=true))
                        TO 'app/db/deaths.parquet' (FORMAT PARQUET);""")
                con.sql("""CREATE OR REPLACE TABLE deaths AS SELECT * FROM 'app/db/deaths.parquet';""")
                #Finally, we lower all column names for ease of access
                #table_name = "deaths"
                #columns = con.sql(f"SELECT * FROM {table_name}").columns
                #for column in columns:
                 #   if isinstance(column, str):
                  #      temp_column_name = column
                   #     column_name_lower = temp_column_name.lower()
                    #    if (temp_column_name != column_name_lower):
                     #       con.sql(f"""ALTER TABLE {table_name} RENAME COLUMN {temp_column_name} TO 
                      #              {column_name_lower}""")
                #if first_chunk:
                  #  first_chunk = False
                con.close()
                return {"status": "Table created from Parquet"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating table: {str(e)}")

@app.get("/clean/clean_csv")
async def clean_csv():
    try:
        csvs_dir = "csv"
        dir_clean_csv = "cleanedCSV/"
        for file in listdir("csv/"):
            if file.endswith(".csv"):
                csv_file_dir = join(csvs_dir, file)
                name_of_cleaned_file = join(dir_clean_csv, file)
                if exists(name_of_cleaned_file):
                    remove(name_of_cleaned_file)
                clean_csv_in_chunks(csv_file_dir, name_of_cleaned_file)
        return {"status": "Succesfully cleaned csvs. You can check them in the cleneadCSV directory."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating table: {str(e)}")


@app.get("/clean/create_csv_table_file")
async def create_csv_table_file():
    try:
        first_chunk = True
        csv_file_dir = ""
        dir_clean_csv = "cleanedCSV"
        csv_to_table = join(dir_clean_csv, "csv_to_table_file.csv")        
        if exists(csv_to_table):
            remove(csv_to_table)
        for file in listdir("cleanedCSV/"):
            if file.endswith(".csv"):
                csv_file_dir = join(dir_clean_csv, file)
                create_csv_from_cleaned(first_chunk, csv_file_dir, csv_to_table)
                first_chunk = False
        return {"status": "Succesfully create table from the cleanedCSV directory. Now you can create a table with the create_table endpoint."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating table: {str(e)}")

@app.get("/clean/columns_to_lower_case")
async def columns_to_lower_case(table_name: str, con: DuckDBConn = Depends(get_db)):
    try:
        columns_to_lower = con.sql(f"SELECT * FROM {table_name}").columns
        for column in columns_to_lower:
            if isinstance(column, str):
                temp_column_name = column
                column_name_lower = temp_column_name.lower()
                if (temp_column_name != column_name_lower):
                    con.sql(f"""ALTER TABLE {table_name} RENAME COLUMN {temp_column_name} TO 
                            {column_name_lower}""")
        result = con.sql(f"SELECT * FROM {table_name}").columns
        return jsonable_encoder({"columns": result})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

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
        result = con.sql(f"SELECT DISTINCT {column1}, {column2} FROM {table};").fetchall()
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


# endpoint que vamos a exponer- 
@app.post("/up")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only csv files allowed")
    try:
        contents = await file.read()
        csv_string = contents.decode('utf-8')
        csv_reader = csv.DictReader(StringIO(csv_string))
        rows = list(csv_reader)
        return contents
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")






# nuestro lado
# function create -> tiene dentro la funcion que limpia los csv
# .   la funcion de clean _> guarda directamente los csv dentro de la carpeta db
#


#El endpoint que vamos a exponer
# necesitamos crear un endpoint que lea el csv
# que unicamente lo descodifique
# y que regrese la respuesta



# lo primero es crear un endpoint que lea un archivo-
# y lo siguente es que lo decodifique con el encoding 
# Y finalmente que lo regrese descodificado


# crear una funcion 




# un clean function que guarda directamente los datos en db
# 



#---
# clean function
#Orientado a guardar los datos dentro del db

#1- modificar el clean para que solo limpie y crear otra funcion y la mandas a llamar para guardar
#para guardar el archivo en db. necesita una bandera para que sepa distinguir si la guarda o no


#---
#1. Hacer que la funcion de clean tome todos lo archivos y lo unique en uno solo, solo leeriamos de un solo
#archivo ya limpiado

#2. Tener varios archivos, pero hacer que la funcion solo los limpie y los guarde con los archivos con
#una nomenglatura:  'cleaned_'

#3. Hacer que la funcion limpie los archivos pero que los guarde en una carpeta 'cleaned_db/', y aqui
#le decimos a duckdb que lea todos los archivos de la carpeta 'cleaned_db/'

#---

#3. Hacer que la funcion limpie los archivos pero que los guarde en una carpeta 'cleaned_db/', y aqui
#le decimos a duckdb que lea todos los archivos de la carpeta 'cleaned_db/'

#'cleaned_db/' -> archivos limpios

#---->
#/create
#llimpia(true)

#/updaload
#llimpia(false)


#function limpia(banderaGuardar,archivo...){
#if banderaGuardar
#    ir a db y guardar el archivo que acaba de limpiar
#else
#    return el archivo modificado
#}
#----->

#/upload
#respuesta = limpiar2(archivo)
#return respuesta

#limpiar2(archivo)
#    leer con el encoding
#    return archivo

#---->


# funcion -orientada no aguardar sino a solo limpiar-



#clean coomo esta y . crear otra funcion que solo se dedique a limpiar




