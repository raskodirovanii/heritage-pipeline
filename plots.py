import os
import re
import textwrap

import matplotlib.pyplot as plt
import pandas as pd


DATA_PATH = "data/processed/exhibits.csv"
OUT_DIR = "reports/figures"
TOP_N = 12  # сколько показывать в топах


def ensure_out_dir() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)


def shorten(text: str, max_len: int = 55) -> str:
    """Аккуратно укорачивает длинные подписи."""
    if not isinstance(text, str):
        return "—"
    t = re.sub(r"\s+", " ", text).strip()
    if len(t) <= max_len:
        return t
    return t[: max_len - 1] + "…"


def wrap(text: str, width: int = 28) -> str:
    """Перенос строки для подписей."""
    if not isinstance(text, str):
        return "—"
    t = re.sub(r"\s+", " ", text).strip()
    return "\n".join(textwrap.wrap(t, width=width))


def savefig(name: str) -> None:
    path = os.path.join(OUT_DIR, name)
    plt.tight_layout()
    plt.savefig(path, dpi=200)
    print("Saved:", path)


def top_barh(series: pd.Series, title: str, xlabel: str, filename: str) -> None:
    """Горизонтальный барчарт с длинными подписями."""
    s = series.dropna()
    s = s[s.astype(str).str.strip() != ""]
    s = s.value_counts().head(TOP_N).sort_values()  # sort для барх: снизу вверх
    labels = [wrap(shorten(str(x), 70), 30) for x in s.index]

    plt.figure(figsize=(12, 7))
    plt.barh(labels, s.values)
    plt.title(title)
    plt.xlabel(xlabel)
    # увеличим левое поле, чтобы подписи не резались
    plt.subplots_adjust(left=0.42)
    savefig(filename)
    plt.close()


def top_bar(series: pd.Series, title: str, ylabel: str, filename: str) -> None:
    """Вертикальные бары — только там, где подписи короткие."""
    s = series.dropna()
    s = s[s.astype(str).str.strip() != ""]
    s = s.value_counts().head(TOP_N)

    labels = [shorten(str(x), 18) for x in s.index]
    plt.figure(figsize=(10, 6))
    plt.bar(labels, s.values)
    plt.title(title)
    plt.ylabel(ylabel)
    plt.xticks(rotation=30, ha="right")
    savefig(filename)
    plt.close()


def pie_missing(series: pd.Series, title: str, filename: str) -> None:
    """Круговая диаграмма: доля пустых/не установлено."""
    s = series.astype(str).fillna("")
    is_missing = (s.str.strip() == "") | (s.str.lower().str.contains("не установлен"))
    counts = pd.Series(
        {
            "Заполнено": int((~is_missing).sum()),
            "Пусто/не установлено": int(is_missing.sum()),
        }
    )

    plt.figure(figsize=(7, 7))
    plt.pie(counts.values, labels=counts.index, autopct="%1.1f%%")
    plt.title(title)
    savefig(filename)
    plt.close()


def hist_name_lengths(series: pd.Series, title: str, filename: str) -> None:
    """Гистограмма длин названий экспонатов (показывает 'шумность' данных)."""
    s = series.dropna().astype(str)
    lengths = s.str.len()

    plt.figure(figsize=(10, 6))
    plt.hist(lengths, bins=40)
    plt.title(title)
    plt.xlabel("Длина названия (символы)")
    plt.ylabel("Количество")
    savefig(filename)
    plt.close()


def main() -> None:
    ensure_out_dir()

    df = pd.read_csv(DATA_PATH)

    # 1) Топ музеев — исправленный график (главный)
    top_barh(
        df["museum_name"],
        title=f"Топ-{TOP_N} музеев по числу объектов (выборка)",
        xlabel="Количество объектов",
        filename="01_top_museums_barh.png",
    )

    # 2) Топ типологий — тоже длинные названия, делаем barh
    top_barh(
        df["typology_name"],
        title=f"Топ-{TOP_N} типологий музейных объектов (выборка)",
        xlabel="Количество объектов",
        filename="02_top_typologies_barh.png",
    )

    # 3) Топ периодов — обычно короткие, но иногда длинные; тоже barh, чтобы не резалось
    top_barh(
        df["periodStr"],
        title=f"Топ-{TOP_N} периодов (periodStr) (выборка)",
        xlabel="Количество объектов",
        filename="03_top_periods_barh.png",
    )

    # 4) Доля 'не установлено' в периодах — очень хороший слайд для качества данных
    pie_missing(
        df["periodStr"],
        title="Заполненность поля periodStr",
        filename="04_period_filled_pie.png",
    )

    # 5) Гистограмма длин названий экспонатов — показывает вариативность и шум
    if "name" in df.columns:
        hist_name_lengths(
            df["name"],
            title="Распределение длины названий объектов",
            filename="05_name_length_hist.png",
        )

    # 6) (опционально) Топ мест производства — подписи могут быть длинные, сделаем barh
    if "productionPlace" in df.columns:
        top_barh(
            df["productionPlace"],
            title=f"Топ-{TOP_N} мест производства (productionPlace) (выборка)",
            xlabel="Количество объектов",
            filename="06_top_places_barh.png",
        )

    print("\nГотово. Картинки лежат в:", OUT_DIR)


if __name__ == "__main__":
    main()
