from typing import List, Optional
from pydantic import BaseModel

class PopulationBatchRequest(BaseModel):
    year: str
    cve_states: List[str] = []
    cvegeos: List[str] = []
    age_group: str = ""
    gender: str = ""
