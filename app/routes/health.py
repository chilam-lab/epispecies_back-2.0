from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def root():
    """Get basic information about the API."""
    return {
        "message": "Multi-CSV Database API",
        "endpoints": [
            "/search - Search records across all databases",
            "/columns - List available columns from all databases",
            "/stats - Get database statistics"
        ]
    }

@router.get("/stats")
async def get_stats():
    """Get combined database statistics."""
    return app.state.db.get_stats()