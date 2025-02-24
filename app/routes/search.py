from fastapi import APIRouter, HTTPException, Depends
from ..services.multi_csv_database import MultiCSVDatabase
from fastapi import Request

router = APIRouter(prefix="/search")

async def get_db(request: Request) -> MultiCSVDatabase:
    return request.app.state.db

@router.get("/columns")
async def get_columns(db: MultiCSVDatabase = Depends(get_db)):
    """Get list of available columns across all databases."""
    return {"columns": db.get_columns()}