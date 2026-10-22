# Технология MapReduce

Даниил Тиуков

Анализ вакансий Java с использованием RustFS, собственного MapReduce и DuckDB. Рассчитаны частоты навыков, пар и троек, проверено совпадение результатов. Выполнены замеры времени и памяти, анализ зарплат и сравнение трёх регионов.

[Отчёт](report.pdf) · [Исходные данные](https://drive.google.com/file/d/1rLEu3N2isIBhAkK1EcSJ2482ouOPAots/view)

## Запуск

Нужны Python 3.12+ и Docker Desktop. Команды для PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Создать файл `.env`, указав логин и пароль RustFS:

```text
S3_ENDPOINT=http://127.0.0.1:19000
RUSTFS_ACCESS_KEY=ваш_логин
RUSTFS_SECRET_KEY=ваш_пароль
```

Можно использовать хранилище из предыдущей работы. Для нового стенда:

```powershell
1..4 | ForEach-Object { New-Item -ItemType Directory -Path "storage/disks/disk$_" -Force | Out-Null }
docker compose up -d
```

Скачать [исходный CSV](https://drive.google.com/file/d/1rLEu3N2isIBhAkK1EcSJ2482ouOPAots/view) в папку `data/` под именем `it_vacancies_2024.csv`. Затем выполнить расчёты:

```powershell
.\.venv\Scripts\python.exe -X utf8 -u lab.py all
```

## Файлы

- `lab.py`, `java.sql`: расчёты, сверка и профилирование.
- `report.pdf`: готовый отчёт.
- `results/`: частоты навыков, зарплатные и региональные таблицы.
- `evidence/`: выводы программ и замеры.
- `screenshots/`: скриншоты RustFS.

Исходный CSV хранится в бакете `hh-lake`. Данные, диски RustFS и `.env` в репозиторий не включены.
