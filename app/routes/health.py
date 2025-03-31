from fastapi import APIRouter

router = APIRouter()

@router.get("/", tags=["Home"])
async def root():
    """Get basic information about the API."""
    return {
        "message": "Multi-CSV Database API",
        "endpoints": [
            "/search - Search records across all databases",
            "/columns - List available columns from all databases",
        ]
    }
