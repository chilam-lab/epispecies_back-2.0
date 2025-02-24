
from typing import List, Union
from ..schemas.database import CSVDatabase
from ..models.health_record import Record

class MultiCSVDatabase:
    def __init__(self, databases: List[CSVDatabase]):
        self.databases = databases
        
    def get_columns(self) -> List[str]:
        """Get unique columns from all databases."""
        columns = set()
        for db in self.databases:
            columns.update(db.get_columns())
        return sorted(list(columns))
        
    def search(self, column: str, value: str, limit: int) -> tuple[int, List[Record]]:
        """Search across all databases."""
        total_matches = 0
        all_records = []
        
        for db in self.databases:
            try:
                matches, records = db.search(column=column, value=value, limit=limit-len(all_records))
                total_matches += matches
                record_objects = [Record(**record) for record in records]
                all_records.extend(record_objects)
                
                if len(all_records) >= limit:
                    break
                    
            except ValueError:
                continue
                
        return total_matches, all_records
    
    def get_unique_values(self, column: str) -> List[Union[str, int, float]]:
        """Get unique values from a specific column across all databases."""
        unique_values = set()
        for db in self.databases:
            try:
                values = db.get_unique_values(column)
                unique_values.update(values)
            except ValueError:
                continue
                
        return sorted(list(unique_values))