# Конвейер Big Data для анализа цифрового культурного наследия РФ

Данные: Государственный каталог Музейного фонда РФ (Parquet, ~3 ГБ, десятки миллионов объектов).

## Архитектура
Raw Parquet → Ingest (проверка) → ETL (DuckDB) → Curated Parquet → Analytics (CSV + графики) → Benchmark

## Скрипты

### 01_ingest_batch.py — проверка источника
```bash
python data/scripts/01_ingest_batch.py --input data/raw/data.parquet
