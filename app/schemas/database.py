import pandas as pd
from typing import Optional, List, Dict, Any, Union
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CSVDatabase:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.df = None
        self.load_data()

    def load_data(self) -> None:
        """Load CSV data into memory using chunks for large files."""
        encodings_to_try = [
            'latin1',      # Also known as iso-8859-1
            'utf-8',       # Standard Unicode encoding
            'cp1252',      # Windows Western European
            'iso-8859-15', # Western European with euro sign
        ]
        
        for encoding in encodings_to_try:
            try:
                logger.info(f"Attempting to load CSV with {encoding} encoding...")
                
                # First, read a small sample to get column names
                sample = pd.read_csv(self.csv_path, nrows=5, encoding=encoding)
                columns = sample.columns
                
                # Create a dtype dictionary for columns with mixed types
                dtypes = {}
                # Treat columns 12 and 13 as strings to avoid mixed type issues
                if len(columns) > 13:
                    dtypes[columns[12]] = str
                    dtypes[columns[13]] = str
                
                chunks = []
                for chunk in pd.read_csv(
                    self.csv_path,
                    chunksize=100000,
                    encoding=encoding,
                    on_bad_lines='warn',
                    low_memory=False,
                    dtype=dtypes
                ):
                    chunks.append(chunk)
                self.df = pd.concat(chunks)
                logger.info(f"Successfully loaded {len(self.df)} records using {encoding} encoding")
                return
            except UnicodeDecodeError:
                logger.warning(f"Failed to decode with {encoding}, trying next encoding...")
                continue
            except Exception as e:
                logger.error(f"Error loading CSV file with {encoding}: {str(e)}")
                continue
        
        raise ValueError("Failed to load CSV file with any of the attempted encodings")

    def search(self, column: str, value: str) -> tuple[int, List[Dict[str, Any]]]:
        """Search for records where column matches value."""
        if column not in self.df.columns:
            raise ValueError(f"Column {column} not found in database")
        
        # Case-insensitive partial match
        mask = self.df[column].astype(str).str.contains(value, case=False, na=False)
        matched_records = self.df[mask]
        total_matches = len(matched_records)
        
        records = matched_records.to_dict('records')
        return total_matches, records

    def get_columns(self) -> List[str]:
        """Return list of available columns."""
        return self.df.columns.tolist()

    def get_unique_values(self, column: str) -> List[Union[str, int, float]]:
        """Get unique values from a specific column."""
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found in database")
        
        # Convert to Python native types before returning
        return self.df[column].unique().tolist()
    
    def get_records_of_year(self, column: str, value: str, limit: Optional[int] = None) -> tuple[int, List[Dict[str, Any]]]:
        """Search for records where column matches value."""
        if column not in self.df.columns:
            raise ValueError(f"Column {column} not found in database")
        
        # Log the search parameters and some data info
        logger.info(f"Searching in column: {column} for value: {value}")
        logger.info(f"Column data type: {self.df[column].dtype}")
        logger.info(f"Sample values from column: {self.df[column].head()}")
        
        # Convert column to string and value to match data type
        mask = self.df[column].astype(str).str.contains(str(value), case=False, na=False)
        
        # Log match info
        matched_records = self.df[mask]
        total_matches = len(matched_records)
        logger.info(f"Found {total_matches} matches")
        
        if total_matches == 0:
            # Log some sample data to help debug
            logger.info(f"Sample of data where matches failed: {self.df[column].head(10).tolist()}")
        
        records = matched_records.to_dict('records')
        return total_matches, records