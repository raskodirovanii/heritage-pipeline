import duckdb

con = duckdb.connect()
rows = con.execute(
    "DESCRIBE SELECT * FROM read_parquet('data/raw/data.parquet') LIMIT 1"
).fetchall()

for r in rows:
    print(r)
