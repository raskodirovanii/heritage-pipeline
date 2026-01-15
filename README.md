# Big Data Pipeline for Cultural Heritage of the Russian Federation

Проект реализует конвейер обработки больших данных для анализа цифрового культурного наследия РФ
на основе Государственного каталога Музейного фонда Российской Федерации.

## Источник данных
Официальный набор данных в формате Apache Parquet, опубликованный на портале hubofdata.ru.

Объём данных: ~3 ГБ  
Количество объектов: более 30 млн

## Архитектура конвейера
Parquet → Ingest → Process (DuckDB) → Analyze → CSV / Analytics

## Используемые технологии
- Python 3.11
- DuckDB
- Apache Parquet
- Pandas
- Visual Studio Code

## Этапы работы

### 1. Ingest
Проверка наличия и целостности исходного Parquet-файла.

### 2. Process
Обработка больших данных с использованием DuckDB:
- выбор нужных колонок
- работа с вложенными структурами
- ограничение выборки

### 3. Analyze
Агрегация и анализ:
- топ музеев
- распределение по историческим периодам
- типологии музейных объектов

## Запуск проекта

```bash
pip install -r requirements.txt
python ingest.py
python process.py
python analyze.py
