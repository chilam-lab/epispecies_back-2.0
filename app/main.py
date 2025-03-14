from fastapi import FastAPI
from .routes import search, health, inegi
from .schemas.database import CSVDatabase
from .services.multi_csv_database import MultiCSVDatabase
import os
import glob
from contextlib import asynccontextmanager

app = FastAPI(title="Multi-CSV Database API")

@asynccontextmanager
async def lifespan(app: FastAPI):
    db_path = os.path.join(os.path.dirname(__file__), "db")
    csv_files = glob.glob(os.path.join(db_path, "*.csv"))
    
    if not csv_files:
        raise Exception("No CSV files found in the db directory")
        
    databases = []
    for csv_file in csv_files:
        try:
            db = CSVDatabase(csv_file)
            databases.append(db)
        except Exception as e:
            print(f"Warning: Failed to load {csv_file}: {str(e)}")
            
    app.state.db = MultiCSVDatabase(databases)
    yield
app.lifespan = lifespan

app.include_router(health.router)
app.include_router(search.router)
app.include_router(inegi.router)