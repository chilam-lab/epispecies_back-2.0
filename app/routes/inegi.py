from fastapi import APIRouter, HTTPException
import httpx
import logging
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/database/inegi")

async def fetch_inegi_variables() -> List[Dict[str, Any]]:
    """
    Fetch variables from the external INEGI API
    """
    url = "https://nutricion.c3.unam.mx/chilam/inegi/variables"
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10.0)
            response.raise_for_status()
            return response.json()
    except httpx.RequestError as e:
        logger.error(f"Error fetching INEGI variables: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail=f"Failed to connect to INEGI API: {str(e)}"
        )
    except httpx.HTTPStatusError as e:
        logger.error(f"INEGI API returned error: {str(e)}")
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"INEGI API error: {e.response.text}"
        )

@router.get("/variables")
async def get_inegi_variables():
    """
    Endpoint to get INEGI variables
    Returns the data from the external INEGI API
    """
    logger.info("Fetching INEGI variables")
    variables = await fetch_inegi_variables()
    logger.info(f"Successfully fetched {len(variables)} variable sets")
    return variables