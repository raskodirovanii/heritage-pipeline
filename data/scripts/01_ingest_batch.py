import argparse
import os
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Проверка источника и подготовка сырого слоя данных (Parquet)"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Путь к исходному Parquet-файлу (например: data/raw/data.parquet)",
    )
    parser.add_argument(
        "--output",
        default="data/output/raw_parquet",
        help="Папка сырого слоя (marker-файл _SUCCESS будет создан здесь)",
    )
    return parser.parse_args()


def human_size(num_bytes: int) -> str:
    step = 1024.0
    units = ["Б", "КБ", "МБ", "ГБ", "ТБ"]
    size = float(num_bytes)
    for unit in units:
        if size < step:
            return f"{size:.2f} {unit}"
        size /= step
    return f"{size:.2f} ПБ"


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Проверка источника данных")
    print(f"Parquet-файл: {input_path}")

    if not input_path.exists():
        raise FileNotFoundError("Файл не найден. Проверь путь --input.")

    size_bytes = os.path.getsize(input_path)
    print(f"Источник найден. Размер: {human_size(size_bytes)}")

    success_path = output_dir / "_SUCCESS"
    success_path.write_text("OK\nИсточник найден и готов к обработке.\n", encoding="utf-8")

    print(f"Создан marker: {success_path}")
    print("Ingest завершён.")


if __name__ == "__main__":
    main()
