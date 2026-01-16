import argparse
import time
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Бенчмарк масштабируемости конвейера обработки данных"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Путь к исходному Parquet (raw или curated)",
    )
    parser.add_argument(
        "--outdir",
        required=True,
        help="Папка для сохранения результатов бенчмарка",
    )
    parser.add_argument(
        "--limits",
        default="50000,100000,200000",
        help="Список LIMIT через запятую (например: 50000,100000,200000)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    input_path = Path(args.input).resolve()
    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    limits = [int(x) for x in args.limits.split(",")]

    print("Бенчмарк масштабируемости обработки данных")
    print(f"Источник данных: {input_path}")
    print(f"Значения LIMIT: {limits}")
    print(f"Выходная папка: {outdir}")
    print()

    con = duckdb.connect()
    results = []

    for lim in limits:
        print(f"Запуск бенчмарка для LIMIT {lim}")

        # COUNT(*)
        t0 = time.perf_counter()
        rows = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{input_path}') LIMIT {lim}"
        ).fetchone()[0]
        t_count = time.perf_counter() - t0

        # Простая агрегация
        t1 = time.perf_counter()
        con.execute(
            f"""
            SELECT COUNT(*) 
            FROM (
                SELECT museum_name
                FROM read_parquet('{input_path}')
                LIMIT {lim}
            )
            """
        ).fetchone()
        t_agg = time.perf_counter() - t1

        total = t_count + t_agg

        print(
            f"  строки={rows}, "
            f"count={t_count:.3f} сек, "
            f"agg={t_agg:.3f} сек, "
            f"итого={total:.3f} сек"
        )

        results.append(
            {
                "limit": lim,
                "rows": rows,
                "time_count_s": t_count,
                "time_agg_s": t_agg,
                "time_total_s": total,
            }
        )

    df = pd.DataFrame(results)
    csv_path = outdir / "benchmark.csv"
    png_path = outdir / "benchmark.png"

    df.to_csv(csv_path, index=False)

    # График
    plt.figure()
    plt.plot(df["rows"], df["time_total_s"], marker="o")
    plt.xlabel("Количество строк")
    plt.ylabel("Время обработки, сек")
    plt.title("Масштабируемость конвейера (время vs объём данных)")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(png_path, dpi=160)
    plt.close()

    print("\nБенчмарк завершён.")
    print(f"CSV: {csv_path}")
    print(f"График: {png_path}")


if __name__ == "__main__":
    main()
