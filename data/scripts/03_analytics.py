import argparse
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Аналитика витрины (curated Parquet): CSV-отчеты и графики"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Путь к curated.parquet или папке с ним",
    )
    parser.add_argument(
        "--outdir",
        required=True,
        help="Папка для результатов (CSV + PNG)",
    )
    return parser.parse_args()


def resolve_input(input_arg: str) -> Path:
    p = Path(input_arg)
    if p.is_dir():
        candidate = p / "curated.parquet"
        if candidate.exists():
            return candidate
        raise FileNotFoundError(
            f"В папке {p} не найден curated.parquet. Укажи путь к файлу."
        )
    return p


def save_df(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def main() -> None:
    args = parse_args()
    in_file = resolve_input(args.input).resolve()
    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    if not in_file.exists():
        raise FileNotFoundError(f"Файл витрины не найден: {in_file}")

    print("Аналитика витрины: построение отчетов и графиков")
    print(f"Витрина: {in_file}")
    print(f"Выходная папка: {outdir}")

    con = duckdb.connect()

    # 0) Общие метрики витрины
    total = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)",
        [str(in_file)],
    ).fetchone()[0]
    save_df(pd.DataFrame([{"rows": total}]), outdir / "summary.csv")

    # 1) Топ музеев по числу объектов
    q1 = con.execute(
        """
        SELECT museum_name, COUNT(*) AS objects
        FROM read_parquet(?)
        WHERE museum_name IS NOT NULL AND TRIM(museum_name) <> ''
        GROUP BY museum_name
        ORDER BY objects DESC
        LIMIT 20
        """,
        [str(in_file)],
    ).fetchdf()
    save_df(q1, outdir / "top_museums.csv")

    # 2) Топ типологий
    q2 = con.execute(
        """
        SELECT typology_name, COUNT(*) AS objects
        FROM read_parquet(?)
        WHERE typology_name IS NOT NULL AND TRIM(typology_name) <> ''
        GROUP BY typology_name
        ORDER BY objects DESC
        LIMIT 20
        """,
        [str(in_file)],
    ).fetchdf()
    save_df(q2, outdir / "top_typologies.csv")

    # 3) Распределение по periodStr (периоды)
    q3 = con.execute(
        """
        SELECT periodStr, COUNT(*) AS objects
        FROM read_parquet(?)
        WHERE periodStr IS NOT NULL AND TRIM(periodStr) <> ''
        GROUP BY periodStr
        ORDER BY objects DESC
        LIMIT 30
        """,
        [str(in_file)],
    ).fetchdf()
    save_df(q3, outdir / "objects_by_period.csv")

    print("Построение графиков...")

    if not q1.empty:
        plt.figure()
        plt.barh(q1["museum_name"][::-1], q1["objects"][::-1])
        plt.title("Топ-20 музеев по числу объектов")
        plt.xlabel("Количество объектов")
        plt.tight_layout()
        plt.savefig(outdir / "top_museums.png", dpi=160)
        plt.close()

    if not q2.empty:
        plt.figure()
        plt.barh(q2["typology_name"][::-1], q2["objects"][::-1])
        plt.title("Топ-20 типологий по числу объектов")
        plt.xlabel("Количество объектов")
        plt.tight_layout()
        plt.savefig(outdir / "top_typologies.png", dpi=160)
        plt.close()

    if not q3.empty:
        plt.figure()
        plt.barh(q3["periodStr"][::-1].astype(str), q3["objects"][::-1])
        plt.title("Топ периодов (periodStr) по числу объектов")
        plt.xlabel("Количество объектов")
        plt.tight_layout()
        plt.savefig(outdir / "objects_by_period.png", dpi=160)
        plt.close()

    print("Готово.")
    print(f"CSV и графики сохранены в: {outdir}")


if __name__ == "__main__":
    main()
