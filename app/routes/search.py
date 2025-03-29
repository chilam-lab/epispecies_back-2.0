from app.models.health_record import Record
from fastapi import APIRouter, HTTPException, Depends
from ..services.multi_csv_database import MultiCSVDatabase
from fastapi import Request
from typing import List, Union, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/search")

async def get_db(request: Request) -> MultiCSVDatabase:
    return request.app.state.db

@router.get("/columns", tags=["Search"])
async def get_columns(db: MultiCSVDatabase = Depends(get_db)):
    """(Obtiene el nombre de los registros de la bd)Get list of available columns across all databases."""
    return {"columns": db.get_columns()}

@router.get("/unique", tags=["Search"])
async def get_unique_values(
    column: str,
    db: MultiCSVDatabase = Depends(get_db)
) -> List[Union[str, int, float]]: 
    """(Obtiene los valores de los registros de la bd)Get unique values from any column."""
    try:
        # Get the correct column name from Record class fields
        record_fields = Record.__annotations__.keys()
        
        # Try to find exact or case-insensitive match
        column_lower = column.lower()
        matching_field = next(
            (field for field in record_fields if field.lower() == column_lower),
            None
        )
        
        if not matching_field:
            raise HTTPException(
                status_code=400,
                detail=f"Column '{column}' not found. Available columns are: {list(record_fields)}"
            )
            
        unique_values = db.get_unique_values(column=matching_field)
        return unique_values
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/unique-pairs", tags=["Search"])
async def get_unique_pairs(
    column1: str,
    column2: str,
    db: MultiCSVDatabase = Depends(get_db)
) -> List[Dict[str, Union[str, int, float]]]:
    """
    Get unique pairs of values from two specified columns.
    Returns a list of dictionaries with the two columns and their corresponding values.
    """
    try:
        record_fields = Record.__annotations__.keys()
        
        column1_lower = column1.lower()
        column2_lower = column2.lower()
        
        matching_field1 = next(
            (field for field in record_fields if field.lower() == column1_lower),
            None
        )
        matching_field2 = next(
            (field for field in record_fields if field.lower() == column2_lower),
            None
        )
        
        if not matching_field1:
            raise HTTPException(
                status_code=400,
                detail=f"Column '{column1}' not found. Available columns are: {list(record_fields)}"
            )
        if not matching_field2:
            raise HTTPException(
                status_code=400,
                detail=f"Column '{column2}' not found. Available columns are: {list(record_fields)}"
            )
            
        unique_pairs = db.get_unique_pairs(matching_field1, matching_field2)
        return unique_pairs
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/year/{year}", tags=["Search"])
async def get_records_by_year(
    year: int,
    db: MultiCSVDatabase = Depends(get_db)
) -> dict:
    """Get all records from a specific year."""
    logger.info(f"Received request for year: {year}")
    total_matches, records = db.get_records_of_year(str(year))
    
    logger.info(f"Returning {total_matches} records")
    return {
        "total": total_matches,
        "records": records
    }