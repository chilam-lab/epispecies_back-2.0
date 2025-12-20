FROM python:3.12-slim
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
RUN mkdir ./duckdb_files/
RUN mkdir ./db/
RUN mkdir ./cleanedCSV/
RUN mkdir ./models/
COPY services/ ./services/
COPY tests/ ./tests/
COPY models/ ./models/
COPY .env .
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
