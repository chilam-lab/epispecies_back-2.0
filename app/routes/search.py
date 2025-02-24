from app.models.health_record import Record
from fastapi import APIRouter, HTTPException, Depends
from ..services.multi_csv_database import MultiCSVDatabase
from fastapi import Request
from typing import List, Union

router = APIRouter(prefix="/search")

async def get_db(request: Request) -> MultiCSVDatabase:
    return request.app.state.db

@router.get("/columns")
async def get_columns(db: MultiCSVDatabase = Depends(get_db)):
    """Get list of available columns across all databases."""
    return {"columns": db.get_columns()}

@router.get("/unique")
async def get_unique_values(
    column: str,
    db: MultiCSVDatabase = Depends(get_db)
) -> List[Union[str, int, float]]:  # Now accepts strings, integers and floats
    """Get unique values from any column."""
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