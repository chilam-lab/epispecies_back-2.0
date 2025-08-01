import pandas as pd
import chardet
from io import StringIO
import os
import unicodedata
import csv

def detect_encoding(file_path, sample_size=1000000):
    """Detect file encoding from a sample."""
    with open(file_path, 'rb') as f:
        sample = f.read(sample_size)
    result = chardet.detect(sample)
    return result['encoding'], result['confidence']

def normalize(string_chunk):
    if isinstance(string_chunk, str):
        norm = unicodedata.normalize('NFD', string_chunk)
        return ''.join([c for c in norm if not unicodedata.combining(c)])
    return string_chunk

def clean_csv_in_chunks(input_path, output_path, chunk_size=100000):
    """Clean CSV in chunks to handle large files."""
    # First, detect encoding
    #encoding, confidence = detect_encoding(input_path)
    #print(f"Detected encoding: {encoding} with confidence: {confidence}")
    
    # Process in chunks
    first_chunk = True
    chunk_no = 0    
    for chunk in pd.read_csv(input_path, chunksize=chunk_size, encoding="utf-8", encoding_errors='replace',
                        on_bad_lines='warn', dtype=str, engine='python', low_memory=True):
        chunk.apply(lambda x: ''.join(ch for ch in str(x) if ord(ch) >= 32 or ch in '\n\r\t'))
        chunk.apply(lambda s: normalize(s))
        mode = 'w' if first_chunk else 'a'
        chunk.to_csv(output_path, mode=mode, index=False, header=first_chunk, encoding='utf-8')
        first_chunk = False
        print(f"Processed chunk {chunk_no+1} ({chunk_size * (chunk_no+1)} rows)")
        chunk_no += 1

def create_csv_from_cleaned(first_chunk, input_path, output_path, chunk_size=100000):
    chunks = pd.read_csv(input_path, chunksize=chunk_size, encoding='utf-8', encoding_errors='replace',
                        on_bad_lines='warn', dtype=str, engine='python', low_memory=True)
    for i, chunk in enumerate(chunks):
        # Write to output file
        mode = 'w' if first_chunk else 'a'
        chunk.to_csv(output_path, mode=mode, index=False, header=first_chunk, encoding='utf-8')
        first_chunk = False
        
        # Status update
        print(f"Processed chunk {i+1} ({chunk_size * (i+1)} rows)")