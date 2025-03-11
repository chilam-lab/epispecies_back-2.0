from typing import Optional, List, Union
from ..schemas.database import CSVDatabase
from ..models.health_record import Record
import logging
import csv

logger = logging.getLogger(__name__)

def create_dynamic_record_class(column_names: List[str]):
    """Create a dynamic Record class based on provided column names."""
    # Clean up column names
    cleaned_columns = [col.strip().replace(' ', '_').lower() for col in column_names]
    print(cleaned_columns)
    
    # Create dynamic class
    class DynamicRecord:
        def __init__(self, **kwargs):
            for col in cleaned_columns:
                value = kwargs.get(col)
                if value is not None:
                    # Optional: Add type conversion
                    try:
                        if str(value).isdigit():
                            value = int(value)
                        elif str(value).replace('.', '').isdigit():
                            value = float(value)
                    except (ValueError, AttributeError):
                        pass
                setattr(self, col, value)
                
        def __str__(self):
            return ', '.join(f"{key}={value}" for key, value in self.__dict__.items())
        
        def to_dict(self):
            """Convert instance to dictionary for compatibility."""
            return self.__dict__.copy()
    
    return type('DynamicRecord', (object,), dict(DynamicRecord.__dict__))

class MultiCSVDatabase:
    def __init__(self, databases: List[CSVDatabase]):
        self.databases = databases
        # Create dynamic record class based on all columns
        self.record_class = create_dynamic_record_class(self.get_columns())
        print(self.record_class)
        
    def get_columns(self) -> List[str]:
        """Get unique columns from all databases."""
        columns = set()
        for db in self.databases:
            columns.update(db.get_columns())
        return sorted(list(columns))
        
    def search(self, column: str, value: str, limit: int = None) -> tuple[int, List['DynamicRecord']]:
        """Search across all databases."""
        total_matches = 0
        all_records = []
        
        for db in self.databases:
            try:
                matches, records = db.search(column=column, value=value)
                total_matches += matches
                # Use dynamic record class instead of static Record
                record_objects = [self.record_class(**record) for record in records]
                all_records.extend(record_objects)
                    
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
    
    def get_records_of_year(self, year: str) -> tuple[int, List['DynamicRecord']]:
        """Get all records for a specific year across all databases."""
        total_matches = 0
        all_records = []
        
        logger.info(f"Searching for year: {year} across all databases")
        
        for db in self.databases:
            try:
                matches, records = db.get_records_of_year(column="Anio", value=year)
                logger.info(f"Found {matches} matches in database")
                total_matches += matches
                # Use dynamic record class instead of static Record
                record_objects = [self.record_class(**record) for record in records]
                all_records.extend(record_objects)
                    
            except ValueError as e:
                logger.warning(f"Error searching database: {str(e)}")
                continue
                
        logger.info(f"Total matches found across all databases: {total_matches}")
        return total_matches, all_records