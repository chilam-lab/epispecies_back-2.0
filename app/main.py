from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware  # Import CORSMiddleware
from .routes import search, health, inegi
from .schemas.database import CSVDatabase
from .services.multi_csv_database import MultiCSVDatabase
import os
import glob
from contextlib import asynccontextmanager
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize the database
    logger.info("Starting up: Initializing MultiCSVDatabase...")
    db_path = os.path.join(os.path.dirname(__file__), "db")
    csv_files = glob.glob(os.path.join(db_path, "*.csv"))
    
    if not csv_files:
        logger.error("No CSV files found in the db directory")
        raise Exception("No CSV files found in the db directory")
        
    databases = []
    for csv_file in csv_files:
        try:
            db = CSVDatabase(csv_file)
            databases.append(db)
            logger.info(f"Successfully loaded CSV file: {csv_file}")
        except Exception as e:
            logger.warning(f"Failed to load {csv_file}: {str(e)}")
            continue  # Continue loading other files even if one fails
            
    if not databases:
        logger.error("No valid CSV databases were loaded")
        raise Exception("No valid CSV databases were loaded")
        
    app.state.db = MultiCSVDatabase(databases)
    logger.info(f"MultiCSVDatabase initialized with {len(databases)} databases")
    
    try:
        yield  # The app runs here
    finally:
        # Shutdown: Clean up
        logger.info("Shutting down: Cleaning up MultiCSVDatabase...")
        app.state.db = None

# Initialize FastAPI app with lifespan
app = FastAPI(title="Multi-CSV Database API", lifespan=lifespan)

# Configure CORS
origins = [
    "http://localhost:4200",  # Allow Angular dev server
    # Add other origins as needed, e.g., "https://your-production-domain.com"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,  # Set to True if you use cookies/auth, False otherwise
    allow_methods=["*"],     # Allow all methods (GET, POST, etc.)
    allow_headers=["*"],     # Allow all headers
)

# Include routers
app.include_router(health.router)
app.include_router(search.router)
app.include_router(inegi.router)