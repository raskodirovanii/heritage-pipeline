import os
import duckdb

RAW = "data/raw/data.parquet"
OUT = "data/processed/exhibits.csv"

os.makedirs("data/processed", exist_ok=True)

con = duckdb.connect()

# Выбираем только нужные поля + ограничиваем объём (для учёбы)
con.execute(
    f"""
    CREATE OR REPLACE TABLE exhibits AS
    SELECT
        id,
        name,
        periodStr,
        productionPlace,
        museum.name AS museum_name,
        typology.name AS typology_name
    FROM read_parquet('{RAW}')
    WHERE name IS NOT NULL
    LIMIT 50000
    """
)

con.execute(
    f"""
    COPY exhibits TO '{OUT}'
    (HEADER, DELIMITER ',');
    """
)

rows = con.execute("SELECT COUNT(*) FROM exhibits").fetchone()[0]
print("Processed rows:", rows)
print("Saved to:", OUT)
