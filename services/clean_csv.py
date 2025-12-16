import pandas as pd
import chardet
from io import StringIO
import unicodedata

def detect_encoding(file_path, sample_size=1000000):
    """Detect file encoding from a sample."""
    sample = b''
    repeats = 10

    if not isinstance(file_path, str):
        while repeats > 0:
            sample += file_path.read(sample_size)
            repeats -= 1
        file_path.seek(0)
    else:
        with open(file_path, 'rb') as f:
            for chunk in read_in_chunks(f, sample_size):
                sample += chunk
                repeats -= 1
                if repeats <= 0:
                    f.close()
                    break
    sample = sample.decode('utf-8', 'replace')
    result = chardet.detect(sample.encode('utf-8', 'replace'))
    return result['encoding']

def read_in_chunks(file_to_read, sample_size):
    while True:
        chunk = file_to_read.read(sample_size)
        if not chunk:
            break
        yield chunk

def clean_csv_in_chunks(input_path, output_path, chunk_size=100000):
    """Clean CSV in chunks to handle large files."""    
    # Process in chunks
    first_chunk = True
    chunk_no = 0
    encode = detect_encoding(input_path)
    print(encode)
    for chunk in pd.read_csv(input_path, chunksize=chunk_size, encoding=encode, encoding_errors='replace',
                        on_bad_lines='warn', dtype=str, engine='python', low_memory=True):
        chunk.apply(lambda x: ''.join(ch for ch in str(x) if ord(ch) >= 32 or ch in '\n\r\t'))
        mode = 'w' if first_chunk else 'a'
        chunk.to_csv(output_path, mode=mode, index=False, header=first_chunk, encoding='utf-8')
        first_chunk = False
        print(f"Processed chunk {chunk_no+1} ({chunk_size * (chunk_no+1)} rows)")
        chunk_no += 1

def db_columns_to_lowercase(table_name, con):
    columns_to_lower = con.sql(f"SELECT * FROM {table_name}").columns
    for column in columns_to_lower:
        if isinstance(column, str):
            temp_column_name = column
            column_name_lower = temp_column_name.lower()
            if (temp_column_name != column_name_lower):
                con.sql(f"""ALTER TABLE {table_name} RENAME COLUMN {temp_column_name} TO 
                            {column_name_lower}""")
