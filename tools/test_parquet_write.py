import pathlib
import duckdb

out_dir = pathlib.Path("data/output/curated_parquet")
out_dir.mkdir(parents=True, exist_ok=True)
out_file = out_dir / "test.parquet"

con = duckdb.connect()
con.execute(
    "COPY (SELECT 1 AS x) TO 'data/output/curated_parquet/test.parquet' (FORMAT PARQUET)"
)

print("exists:", out_file.exists())
if out_file.exists():
    print("size:", out_file.stat().st_size)
