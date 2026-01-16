import argparse
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ETL: извлечение и очистка данных из raw Parquet в curated Parquet"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Путь к исходному Parquet (например: data/raw/data.parquet)",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Путь к папке curated Parquet (например: data/output/curated_parquet)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200000,
        help="Ограничение по строкам для учебной выборки. 0 = обработать всё.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    input_path = Path(args.input).resolve()
    out_dir = Path(args.output).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "curated.parquet"

    if not input_path.exists():
        raise FileNotFoundError(f"Исходный Parquet не найден: {input_path}")

    limit_sql = "" if args.limit == 0 else f"LIMIT {args.limit}"

    print("Преобразование данных (ETL): формирование аналитической витрины")
    print(f"Текущая папка запуска: {Path.cwd().resolve()}")
    print(f"Чтение raw Parquet: {input_path}")
    print(f"Запись curated Parquet: {out_file}")
    print("Режим:", "полный объём" if args.limit == 0 else f"LIMIT {args.limit}")

    con = duckdb.connect()

    # 1) Считаем, сколько строк попадёт в витрину
    cnt = con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{input_path.as_posix()}')
        WHERE name IS NOT NULL
          AND TRIM(CAST(name AS VARCHAR)) <> ''
        """
    ).fetchone()[0]
    print(f"Строк после базовой очистки: {cnt}")

    if cnt == 0:
        raise RuntimeError(
            "После фильтрации получилось 0 строк. Проверь качество данных и условия фильтра."
        )

    # 2) Пишем Parquet напрямую из SELECT (самый надежный способ)
    print("Запись файла витрины...")

    con.execute(
        f"""
        COPY (
            SELECT
                id,
                name,
                productionPlace,
                description,
                partsCount,
                regNumber,
                invNumber,
                gikNumber,
                type,
                statusId,
                museum."name"        AS museum_name,
                museum.code          AS museum_code,
                typology."name"      AS typology_name,
                typology.code        AS typology_code,
                typology.obsolete    AS typology_obsolete,
                periodStr,
                startDate,
                finishDate,
                "precision"          AS date_precision,
                dimStr,
                dimUnit."name"       AS dim_unit,
                width,
                length,
                height,
                weight,
                weightUnit,
                authors,
                technologies,
                findPlace,
                mainWords,
                provenance,
                extUrl,
                fund
            FROM read_parquet('{input_path.as_posix()}')
            WHERE name IS NOT NULL
              AND TRIM(CAST(name AS VARCHAR)) <> ''
            {limit_sql}
        )
        TO '{out_file.as_posix()}'
        (FORMAT PARQUET);
        """
    )

    if not out_file.exists():
        raise RuntimeError(
            f"Файл Parquet не создан: {out_file}. Проверь права записи и путь."
        )

    print("Сохранение завершено успешно.")
    print(f"Файл витрины создан: {out_file}")
    print(f"Размер файла: {out_file.stat().st_size} байт")


if __name__ == "__main__":
    main()
