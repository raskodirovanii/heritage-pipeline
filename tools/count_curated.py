import duckdb

con = duckdb.connect()
cnt = con.execute(
    "SELECT COUNT(*) FROM read_parquet('data/output/curated_parquet/curated.parquet')"
).fetchone()[0]
print("rows_in_curated:", cnt)
