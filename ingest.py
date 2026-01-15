import os

path = "data/raw/data.parquet"

if not os.path.exists(path):
    raise FileNotFoundError("Файл data.parquet не найден")

print("Ingest step completed. Source file found:", path)
