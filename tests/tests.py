import pytest
import duckdb
from fastapi.testclient import TestClient
import sys
import os
import pandas as pd
import tempfile
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from main import app, get_db
from fastapi import HTTPException
from unittest.mock import patch
import json
from unittest.mock import mock_open, patch
from services.clean_csv import detect_encoding, normalize, clean_csv_in_chunks, db_columns_to_lowercase

# Fixture for in-memory DuckDB database
@pytest.fixture
def in_memory_db():
    conn = duckdb.connect(":memory:")
    # Setup test data
    conn.execute("""
        CREATE TABLE RAWDATA (
            id INTEGER,
            name VARCHAR,
            cause VARCHAR,
            Anio VARCHAR,
            CVE_Grupo VARCHAR,
            Grupo VARCHAR,
            CVE_Enfermedad VARCHAR,
            CVE_Causa_def VARCHAR,
            Causa_def VARCHAR
        );
        INSERT INTO RAWDATA VALUES
            (1, 'John', 'Heart Disease', '2020', 'G1', 'Group1', 'E1', 'C1', 'Cause1'),
            (2, 'Jane', 'Cancer', '2021', 'G2', 'Group2', 'E1', 'C2', 'Cause2'),
            (3, 'Doe', 'Heart Disease', '2020', 'G3', 'Group3', 'E2', 'C3', 'Cause3');
    """)
    conn.execute("""
        CREATE TABLE CATEGORIES (
            cvegeo INTEGER,
            anio VARCHAR,
            mes INTEGER,
            indice VARCHAR,
            valor INTEGER,
            categoria VARCHAR
        );
        INSERT INTO CATEGORIES VALUES
            (1001, '2000', 2, 'I1', 1, 'C2'),
            (1001, '2000', 2, 'I1', 1, 'C1'),
            (1001, '2000', 2, 'I1', 1, 'C1'),
            (2, '2013', 12, 'I2', 2, 'C2'),
            (1001, '2000', 2, 'I1', 9, 'C1'),
            (1001, '2000', 2, 'I1', 9, 'C1'),
            (1, '2019', 10, 'I4', 12, 'C1'),
            (1, '2015', 6, 'I5', 120, 'C3');
    """)
    conn.execute("""
        CREATE TABLE DEFUNCIONES (
            id INTEGER,
            name VARCHAR,
            cause VARCHAR,
            anio INTEGER,
            CVE_Grupo VARCHAR,
            Grupo VARCHAR,
            cve_enfermedad INTEGER,
            CVE_Causa_def VARCHAR,
            Causa_def VARCHAR,
            cvegeo VARCHAR
        );
        INSERT INTO DEFUNCIONES VALUES
            (1, 'John', 'Heart Disease', 2000, 'G1', 'Group1', 1, 'C1', 'Cause1', '1001'),
            (2, 'Jane', 'Cancer', 2019, 'G2', 'Group2', 1, 'C2', 'Cause2', '1'),
            (2, 'Jane', 'Cancer', 2000, 'G2', 'Group2', 1, 'C1', 'Cause2', '1001'),
            (2, 'Jane', 'Cancer', 2000, 'G2', 'Group2', 1, 'C1', 'Cause2', '1001'),
            (3, 'Doe', 'Heart Disease', 2015, 'G3', 'Group3', 2, 'C3', 'Cause3', '1');
    """)
    conn.execute("""
        CREATE TABLE ENFERMEDADES (
            cve_grupo VARCHAR,
            grupo VARCHAR,
            cve_enfermedad VARCHAR,
            enfermedad VARCHAR,
            cve_causa_def VARCHAR,
            causa_def VARCHAR
        );
        INSERT INTO ENFERMEDADES VALUES
            ('1', 'Group1', '1', 'Enf1', '1', 'Cause1'),
            ('2', 'Group2', '1', 'Enf2', '2', 'Cause2'),
            ('3', 'Group3', '2', 'Enf3', '3', 'Cause3');
    """)
    conn.execute("""
        CREATE TABLE ESTADO_MUN (
            cve_estado VARCHAR,
            estado VARCHAR,
            cvegeo VARCHAR,
            municipio VARCHAR
        );
        INSERT INTO ESTADO_MUN VALUES
            ('9', 'CDMX', '1001', '8'),
            ('9', 'CDMX', '1001', '8'),
            ('9', 'CDMX', '1001', '8'),
            ('1001', '2000', 'MUJERES', '14');
    """)

    conn.execute("""
        CREATE TABLE POPULATION (
            cvegeo VARCHAR,
            anio VARCHAR,
            sexo VARCHAR,
            edad_gpo VARCHAR,
            poblacion INTEGER
        );
        INSERT INTO POPULATION VALUES
            ('1001', '2000', 'HOMBRES', '65', 10),
            ('1001', '2000', 'MUJERES', '65', 12),
            ('1001', '2000', 'HOMBRES', '14', 15),
            ('1001', '2000', 'MUJERES', '14', 20);
    """)
    conn.execute("""
        CREATE TABLE POPULATION_AGE (
            cvegeo VARCHAR,
            anio VARCHAR,
            edad_gpo VARCHAR,
            poblacion INTEGER
        );
        INSERT INTO POPULATION_AGE VALUES
            ('1001', '2000', '65', 10),
            ('1001', '2000', '65', 12),
            ('1001', '2000', '14', 15),
            ('1001', '2000', '14', 20);
    """)
    conn.execute("""
        CREATE TABLE POPULATION_GENDER (
            cvegeo VARCHAR,
            anio VARCHAR,
            sexo VARCHAR,
            poblacion INTEGER
        );
        INSERT INTO POPULATION_GENDER VALUES
            ('1001', '2000', 'HOMBRES', 10),
            ('1001', '2000', 'MUJERES', 12),
            ('1001', '2000', 'HOMBRES', 15),
            ('1001', '2000', 'MUJERES', 50);
    """)
    conn.execute("""
        CREATE TABLE POPULATION_TOTAL (
            cvegeo VARCHAR,
            anio VARCHAR,
            total_population INTEGER
        );
        INSERT INTO POPULATION_TOTAL VALUES
            ('1001', '2000', 10),
            ('1001', '2000', 20),
            ('1001', '2000', 25),
            ('1001', '2000', 45);
    """)

    conn.execute("""
        CREATE TABLE VAR_DISEASES (
            id VARCHAR,
            enfermedad VARCHAR,
            cve_enfermedad VARCHAR
        );
        INSERT INTO VAR_DISEASES VALUES
            ('EN400', 'Enfermedades respiratorias 2000', '9'),
            ('EN401', 'Enfermedades cardiovasculares 2000', '4'),
            ('EN402', 'Enfermedades neurologicas 2000', '10'),
            ('EN403', 'Enfermedades respiratorias 2000', '9'),
            ('EN404', 'Enfermedades cardiovasculares 2000', '4'),
            ('EN405', 'Enfermedades neurologicas 2000', '10'),
            ('EN406', 'Enfermedades respiratorias 2000', '9'),
            ('EN407', 'Enfermedades cardiovasculares 2000', '4'),
            ('EN408', 'Enfermedades neurologicas 2000', '10'),
            ('EN409', 'Enfermedades neurologicas 2000', '10'),
            ('EN410', 'Transtornos mentales 2000', '12');
    """)
    conn.execute("""
        CREATE TABLE DATA_VAR_DISEASES (
            id VARCHAR,
            cve_enfermedad VARCHAR,
            anio INTEGER,
            cvegeo VARCHAR,
            count INTEGER
        );
        INSERT INTO DATA_VAR_DISEASES VALUES
            ('EN400', '9', 2000, '1001', 10),
            ('EN400', '4', 2000, '1002', 10),
            ('EN400', '10', 2000, '1003', 10),
            ('EN400', '9', 2000, '1004', 10),
            ('EN400', '4', 2000, '1005', 10),
            ('EN400', '10', 2000, '1006', 10),
            ('EN400', '9', 2000, '1007', 10),
            ('EN400', '4', 2000, '1008', 10),
            ('EN400', '10', 2000, '1009', 10),
            ('EN400', '10', 2000, '1010', 10),
            ('EN400', '12', 2000, '1011', 10);
    """)
    conn.execute("""
        CREATE TABLE VAR_GROUP (
            id VARCHAR,
            grupo VARCHAR,
            cve_grupo VARCHAR
        );
        INSERT INTO VAR_GROUP VALUES
            ('GR400', 'Grupo 1 2000', '9'),
            ('GR401', 'Grupo 2 2000', '4'),
            ('GR402', 'Grupo 3 2000', '10'),
            ('GR403', 'Grupo 1 2000', '9'),
            ('GR404', 'Grupo 2 2000', '4'),
            ('GR405', 'Grupo 3 2000', '10'),
            ('GR406', 'Grupo 1 2000', '9'),
            ('GR407', 'Grupo 2 2000', '4'),
            ('GR408', 'Grupo 3 2000', '10'),
            ('GR409', 'Grupo 3 2000', '10'),
            ('GR410', 'Grupo 4 2000', '12');
    """)
    conn.execute("""
        CREATE TABLE DATA_VAR_GROUP (
            id VARCHAR,
            cve_grupo VARCHAR,
            anio INTEGER,
            cvegeo VARCHAR,
            count INTEGER
        );
        INSERT INTO DATA_VAR_GROUP VALUES
            ('GR400', '9', 2000, '1001', 10),
            ('GR400', '4', 2000, '1002', 10),
            ('GR400', '10', 2000, '1003', 10),
            ('GR400', '9', 2000, '1004', 10),
            ('GR400', '4', 2000, '1005', 10),
            ('GR400', '10', 2000, '1006', 10),
            ('GR400', '9', 2000, '1007', 10),
            ('GR400', '4', 2000, '1008', 10),
            ('GR400', '10', 2000, '1009', 10),
            ('GR400', '10', 2000, '1010', 10),
            ('GR400', '12', 2000, '1011', 10);
    """)
    conn.execute("""
        CREATE TABLE VAR_CAUSEDEATH (
            id VARCHAR,
            causa_def VARCHAR,
            cve_causa_def VARCHAR
        );
        INSERT INTO VAR_CAUSEDEATH VALUES
            ('A400', 'def 1 2000', '9'),
            ('B401', 'def 2 2000', '4'),
            ('I402', 'def 3 2000', '10'),
            ('H403', 'def 4 2000', '12');
    """)
    conn.execute("""
        CREATE TABLE DATA_VAR_CAUSEDEATH (
            id VARCHAR,
            cve_causa_def VARCHAR,
            anio INTEGER,
            cvegeo VARCHAR,
            count INTEGER
        );
        INSERT INTO DATA_VAR_CAUSEDEATH VALUES
            ('A400', '9', 2000, '1001', 10),
            ('B401', '4', 2000, '1001', 10),
            ('I402', '10', 2000, '1001', 10),
            ('C403', '12', 2000, '1001', 10);
    """)
    # Attach your original database
    # conn.execute("ATTACH 'duckdb_files/my_database.db' AS orig")

    # # Copy tables we need
    # conn.execute("CREATE OR REPLACE TABLE VAR_DISEASES AS SELECT * FROM orig.VAR_DISEASES")
    # conn.execute("CREATE OR REPLACE TABLE VAR_GROUP AS SELECT * FROM orig.VAR_GROUP")
    # conn.execute("CREATE OR REPLACE TABLE VAR_CAUSEDEATH AS SELECT * FROM orig.VAR_CAUSEDEATH")

    # conn.execute("CREATE OR REPLACE TABLE DATA_VAR_DISEASES AS SELECT * FROM orig.DATA_VAR_DISEASES")
    # conn.execute("CREATE OR REPLACE TABLE DATA_VAR_GROUP AS SELECT * FROM orig.DATA_VAR_GROUP")
    # conn.execute("CREATE OR REPLACE TABLE DATA_VAR_CAUSEDEATH AS SELECT * FROM orig.DATA_VAR_CAUSEDEATH")
    yield conn
    conn.close()

# Fixture to override get_db dependency
@pytest.fixture
def override_get_db(in_memory_db):
    def _get_db():
        return in_memory_db
    app.dependency_overrides[get_db] = _get_db
    yield
    app.dependency_overrides.clear()

# Fixture for TestClient
@pytest.fixture
def client(override_get_db):
    return TestClient(app)

# Test for / endpoint
def test_root(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Hello World"}

# Test for /show endpoint
def test_show_tables(client):
    response = client.get("/show/tables")
    assert response.status_code == 200
    data = response.json()
    assert "tables" in data
    assert any(table["name"] == "RAWDATA" for table in data["tables"])
    assert any(table["name"] == "ENFERMEDADES" for table in data["tables"])

#Test for checking encoding without an initial file path
def test_check_encoding():
    file_content = "Hola Mundo"
    chardet_result = {"encoding": "utf-8", "confidence": 0.99}
    with patch("builtins.open", mock_open(read_data= file_content)):
        with patch("chardet.detect", return_value= chardet_result) as mock_detect:
            encoding, confidence = detect_encoding("dummy_file.txt", sample_size=1000)
    assert encoding == "utf-8"
    assert confidence == 0.99
    mock_detect.assert_called_with(file_content)

#Test for checking encoding with an initial file path
def test_check_encoding_with_file_path():
    file_content = "tests/csvs/Prueba1.csv"
    chardet_result = {"encoding": "utf-8", "confidence": 0.99}
    with patch("builtins.open", mock_open(read_data= file_content)):
        with patch("chardet.detect", return_value= chardet_result) as mock_detect:
            encoding, confidence = detect_encoding(file_content, sample_size=1000)
    assert encoding == "utf-8"
    assert confidence == 0.99
    mock_detect.assert_called_with(file_content)

#Test for checking accent or special characters removal (Normalize)
def test_check_accent_removal():
    string_to_check = "Corazón"
    string_converted = normalize(string_to_check)
    assert string_converted == "Corazon"

#Test for checking accent or special characters removal (Normalize) fialure
def test_check_accent_removal_fail():
    string_to_check = "Corazón"
    string_converted = normalize(string_to_check)
    with pytest.raises(AssertionError):
        assert string_converted == "Corazón"

# Succesfull test for /clean/column endpoint
#def test_columns_to_lower_case_success():
#    tn = "RAWDATA"
#    db_columns_to_lowercase(tn, in_memory_db[get_db])
#    assert "columns" in data
#    assert set(data["columns"]) == {"id", "name", "cause", "anio", "cve_grupo", "grupo", "cve_enfermedad", "cve_causa_def", "causa_def"}

# Test for /clean/column endpoint that check for failure
#def test_columns_to_lower_case_fail(client):
#    response = client.get("/clean/columns_to_lower_case?table_name=RAWDATA")
#    assert response.status_code == 200
#    data = response.json()
#    assert "columns" in data
#    with pytest.raises(AssertionError):
#        assert set(data["columns"]) == {"Id", "Name", "Cause", "Anio", "CVE_Grupo", "Grupo", "CVE_Enfermedad", "CVE_Causa_def", "Causa_def"}

# Test for /columns endpoint
def test_get_columns(client):
    response = client.get("/show/columns?table_name=RAWDATA")
    assert response.status_code == 200
    data = response.json()
    assert "columns" in data
    assert set(data["columns"]) == {"id", "name", "cause", "Anio", "CVE_Grupo", "Grupo", "CVE_Enfermedad", "CVE_Causa_def", "Causa_def"}

# Test for /columns with invalid table
def test_get_columns_invalid_table(client, in_memory_db):
    # Simulate a missing table
    in_memory_db.execute("DROP TABLE RAWDATA")
    response = client.get("/show/columns?table_name=RAWDATA")
    assert response.status_code == 500
    assert "Query error" in response.json()["detail"]

# Test for /unique_columns endpoint
def test_get_unique_columns(client):
    response = client.get("/unique_pair_columns?column1=name&column2=cause&table=RAWDATA")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    assert ["John", "Heart Disease"] in data

# Test for /unique_columns with invalid column
def test_get_unique_columns_invalid_column(client):
    response = client.get("/unique_pair_columns?column1=invalid&column2=cause&table=ENFERMEDADES")
    assert response.status_code == 500
    assert "not found" in response.json()["detail"]

# Test for /get_unique endpoint
def test_get_unique_values(client):
    response = client.get("/unique_values_by_column?column_name=cause&table=RAWDATA")
    assert response.status_code == 200
    data = response.json()
    assert ["Heart Disease"] in data
    assert ["Cancer"] in data

# Test for /get_unique with invalid column
def test_get_unique_values_invalid_column(client):
    response = client.get("/unique_values_by_column?column_name=invalid&table=ENFERMEDADES")
    assert response.status_code == 500
    assert "not found" in response.json()["detail"]


# Test for /get_second_class endpoint
def test_get_second_class(client: TestClient):
    # Mock environment variables for the test
    env_vars = {
        "ID_FIRST_CLASS": "CVE_Enfermedad",
        "FIRST_CLASS_DESCRIPTION": "Enfermedad",
        "ID_SECOND_CLASS": "CVE_Grupo",
        "SECOND_CLASS_DESCRIPTION": "Grupo",
        "TABLE_CLASS": "ENFERMEDADES"
    }
    
    with patch.dict(os.environ, env_vars):
        response = client.get("/get_second_level_class?search_id_first_class=1")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert ["1", "Group1"] in data
        assert ["2", "Group2"] in data
        

# Test for /get_second_class with no data
def test_get_second_class_no_data(client):
    env_vars = {
        "ID_FIRST_CLASS": "CVE_Enfermedad",
        "FIRST_CLASS_DESCRIPTION": "Enfermedad",
        "ID_SECOND_CLASS": "CVE_Grupo",
        "SECOND_CLASS_DESCRIPTION": "Grupo",
        "TABLE_CLASS": "ENFERMEDADES"
    }
    with patch.dict(os.environ, env_vars):
        response = client.get("/get_second_level_class?search_id_first_class=invalid")
        assert response.status_code == 200
        assert response.json() == {"message": "No data found"}

def test_get_second_class_missing_param(client):
    response = client.get("/get_second_level_class")
    assert response.status_code == 422
    assert any(error["type"] == "missing" for error in response.json()["detail"])

def test_get_third_class_no_data(client: TestClient):
    env_vars = {
        "ID_FIRST_CLASS": "CVE_Enfermedad",
        "FIRST_CLASS_DESCRIPTION": "Enfermedad",
        "ID_SECOND_CLASS": "CVE_Grupo",
        "SECOND_CLASS_DESCRIPTION": "Grupo",
        "TABLE_CLASS": "ENFERMEDADES"
    }
    with patch.dict(os.environ, env_vars):
        response = client.get("/get_third_level_class?search_id_first_class=E1&search_id_second_class=G1")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1 
        assert ["C1", "Cause1"] in data

# Test for /get_third_class with no data
def test_get_third_class_no_data(client: TestClient):
    env_vars = {
        "ID_FIRST_CLASS": "CVE_Enfermedad",
        "FIRST_CLASS_DESCRIPTION": "Enfermedad",
        "ID_SECOND_CLASS": "CVE_Grupo",
        "SECOND_CLASS_DESCRIPTION": "Grupo",
        "TABLE_CLASS": "ENFERMEDADES"
    }
    with patch.dict(os.environ, env_vars):
        response = client.get("/get_third_level_class?search_id_first_class=E1&search_id_second_class=Invalid")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}. Response: {response.text}"
        assert response.json() == {"message": "No data found"}, f"Expected 'No data found', got {response.json()}"

def test_get_third_class_missing_params(client: TestClient):
    response = client.get("/get_third_level_class")
    assert response.status_code == 422, f"Expected 422, got {response.status_code}. Response: {response.text}"
    assert any(error["type"] == "missing" for error in response.json()["detail"]), "Expected 'missing' error type"

# Test for /calculate_variables endpoint
def test_calculate_variables(client):
    response = client.get("/calculate_variables?category=C1&year=2000&cve_enfermedad=1")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    assert type(data) == list
    assert data[0]["nx"] == 57
    assert data[0]["n"] == 57

# Test for /get_all_population endpoint
def test_get_all_population(client):
    response = client.get("/get_all_population?year=2000")
    assert response.status_code == 200
    data = response.json()
    assert data == 57

# Test for /get_population endpoint
def test_get_population(client):
    response = client.get("/get_population?year=2000&cvegeo=1001")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    assert ["HOMBRES", "65", 10] in data

# Test for /get_population with no parameters endpoint
def test_get_population_no_params(client):
    response = client.get("/get_population?")
    with pytest.raises(AssertionError):
        assert response.status_code == 400

# Test for /get_population with all parameters endpoint
def test_get_population_all(client):
    response = client.get("/get_population?year=2000&cvegeo=1001&edad_gpo=65&sexo=HOMBRES")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    assert [10] in data

# Test for /get_population with age parameter endpoint
def test_get_population_age_success(client):
    response = client.get("/get_population?year=2000&cvegeo=1001&edad_gpo=65")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    assert ["65", 22] in data

# Test for /get_population with age parameter endpoint
def test_get_population_age_Failure(client):
    response = client.get("/get_population?year=2000&cvegeo=1001&edad_gpo=14")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    with pytest.raises(AssertionError):
        assert ["14", 10] in data

# Test for /get_population with gender parameter endpoint
def test_get_population_gender_success(client):
    response = client.get("/get_population?year=2000&cvegeo=1001&sexo=MUJERES")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    assert ["MUJERES", 62] in data

# Test for /get_population with gender parameter endpoint
def test_get_population_gender_Failure(client):
    response = client.get("/get_population?year=2000&cvegeo=1001&sexo=HOMBRES")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    with pytest.raises(AssertionError):
        assert ["HOMBRES", 30] in data

# Test for /variables endpoint
def test_get_variables(client):
    response = client.get("/variables")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    assert type(data) == list
    assert any(enfermedad["name"] == "Enfermedades respiratorias 2000" and enfermedad["id"] == "EN400" for enfermedad in data)
    
    num_of_diseases = 0
    num_of_group = 0
    num_of_cause = 0
    for ids in data:
        if ids["id"].startswith("EN"):
            num_of_diseases += 1
        elif ids["id"].startswith("GR"):
            num_of_group += 1
        else:
            num_of_cause += 1
    assert num_of_diseases == 11
    assert num_of_group == 11
    assert num_of_cause == 4
    
def test_upload_csv(client: TestClient):
    csv_content = "id,name,cve,description\n1,test,10,Otras formas de enfermedad del corazon"
    response = client.post(
        "/upload_csv",
        files={"file": ("test.csv", csv_content, "text/csv")}
    )
    assert response.status_code == 200

# Test for /variables_id with EN400 parameter endpoint
def test_get_variables_id(client):
    response = client.get("/variables/id?id=EN400")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    assert type(data) == list
    assert any(enfermedad["id"] == "EN400" and enfermedad["bin"] == 9 for enfermedad in data)

# Test for /variables_id with GR6200 parameter endpoint
def test_get_variables_id_gr(client):
    response = client.get("/variables/id?id=GR400")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    assert type(data) == list
    assert any(enfermedad["level_id"] == "GR400-9" and enfermedad["bin"] == 9 for enfermedad in data)

def test_clean_csv_in_chunks(client):
    file_content = "tests/csvs/Prueba1.csv"
    output_file = "tests/csvs/Prueba1C.csv"
    chardet_result = {"encoding": "utf-8", "confidence": 0.99}
    with patch("builtins.open", mock_open(read_data= file_content)):
        with patch("chardet.detect", return_value= chardet_result) as mock_detect:
            encoding, confidence = detect_encoding(file_content, sample_size=1000)
    assert encoding == "utf-8"
    assert confidence == 0.99
    mock_detect.assert_called_with(file_content)

def test_clean_csv_in_chunks(tmp_path, monkeypatch):
    """Test the clean_csv_in_chunks function with various scenarios."""

    input_file = tmp_path / "test_input.csv"
    output_file = tmp_path / "test_output.csv"

    test_csv_content = """id,name,description,year
    1,"John Doe","Normal data",2020
    2,"Jane Smith","Data with special chars: àáâãäå",2021
    3,"Bob Johnson","Control char data\x00\x01",2022
    4,"Alice Brown","Mixed encoding test: café résumé",2023
    """

    with open(input_file, 'w', encoding='utf-8') as f:
        f.write(test_csv_content)

    def mock_detect_encoding(file_path, sample_size=1000000):
        return 'utf-8', 0.95

    monkeypatch.setattr("services.clean_csv.detect_encoding", mock_detect_encoding)
    clean_csv_in_chunks(str(input_file), str(output_file), chunk_size=2)
    assert output_file.exists()
    output_df = pd.read_csv(output_file, encoding='utf-8')
    assert len(output_df) == 4
    expected_columns = ['id', 'name', 'description', 'year']
    assert list(output_df.columns) == expected_columns

    assert output_df['id'].tolist() == [1, 2, 3, 4]
    assert 'John Doe' in output_df['name'].values
    assert 'Jane Smith' in output_df['name'].values
