import argparse
from pathlib import Path

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Потоковая загрузка данных (streaming ingest) в raw Parquet"
    )
    parser.add_argument(
        "--input_dir",
        required=True,
        help="Папка, куда поступают новые Parquet-файлы",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        help="Папка для сохранения raw Parquet (stream)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("heritage_ingest_stream")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    print("Потоковая загрузка данных (streaming ingest)")
    print(f"Источник данных: {input_dir}")
    print(f"Выходная папка: {output_dir}")

    # Spark Structured Streaming
    df_stream = (
        spark.readStream
        .format("parquet")
        .load(str(input_dir))
    )

    query = (
        df_stream.writeStream
        .format("parquet")
        .option("checkpointLocation", str(output_dir / "_checkpoints"))
        .outputMode("append")
        .start(str(output_dir))
    )

    print("Поток запущен. Ожидание новых данных...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
