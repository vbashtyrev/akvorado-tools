# akvorado-tools

Утилиты для работы с Akvorado (ClickHouse): сравнение перцентиля с Zabbix, только Akvorado, удаление данных за период.

Подключение к ClickHouse по SSH-туннелю (порт 8123 на удалённом хосте пробрасывается локально).

## Требования

- Python 3.6+
- `requests`
- SSH-доступ к хосту Akvorado (ключ по умолчанию `~/.ssh/proCloud`)
- Для режима Zabbix / сравнения: `ZABBIX_URL`, `ZABBIX_TOKEN`

```bash
pip install -r requirements.txt
```

---

## 1. `zabbix_percentile.py` — перцентиль и сравнение

95% перцентиль по истории Zabbix за период; опционально сравнение с Akvorado; режим «только Akvorado» без Zabbix.

### Режимы

| Режим | Описание |
|-------|----------|
| Zabbix | `--host`, `--from`, `--to`, `--interface` или `--key` — перцентиль по item'у Zabbix |
| Zabbix + Akvorado | + `--akvorado` — тот же период, вывод Zabbix и Akvorado, сравнение |
| Только Akvorado | `--akvorado-only --akvorado-boundary-only` (или `--akvorado-in-if`) + `--from` / `--to` — без Zabbix |
| Discover таблиц | `--akvorado-discover-boundary` — список таблиц с InIfBoundary=external и min/max времени |
| По всем таблицам | `--akvorado-only --akvorado-boundary-only --akvorado-all-tables` — перцентиль по каждой таблице |

### Основные ключи

- `--host` — хост в Zabbix (обязателен без `--akvorado-only` / `--akvorado-discover-boundary`)
- `--from`, `--to` — период (YYYYMMDD или YYYYMMDDHHMM)
- `--interface` / `--key` — интерфейс или ключ item'а
- `--akvorado-boundary-only` — только InIfBoundary = external (без ExporterName/InIfName)
- `--akvorado-table` — таблица ClickHouse (например `default.flows_5m0s`)
- `--akvorado-discover` — таблицы по ExporterName/InIfName (нужны `--host`, `--akvorado-in-if`)

### Примеры

```bash
# Только Akvorado, InIfBoundary=external
python zabbix_percentile.py --akvorado-only --akvorado-boundary-only --from 202602170805 --to 202602170905

# Список таблиц и min/max по каждой
python zabbix_percentile.py --akvorado-discover-boundary

# Перцентиль по каждой таблице за период
python zabbix_percentile.py --akvorado-only --akvorado-boundary-only --akvorado-all-tables --from 202602170805 --to 202602170905

# Zabbix + сравнение с Akvorado (нужны ZABBIX_URL, ZABBIX_TOKEN)
python zabbix_percentile.py --host MSK-M9-MX204-1 --interface 635 --from 20260101 --to 20260201 --akvorado
```

---

## 2. `akvorado_delete_period.py` — удаление за период

Удаление данных за период: предпросмотр количества строк по каждой таблице, запрос подтверждения (`yes`/no), затем `ALTER TABLE ... DELETE WHERE ...`.

| Ключ | Описание |
|------|----------|
| `--from`, `--to` | Период (YYYYMMDD или YYYYMMDDHHMM). Обязательны. |
| `--table` | Таблица (например `default.flows`). Можно указать несколько раз. |
| `--boundary-only` | Удалять только строки с `InIfBoundary = 'external'`. |
| `--yes` | Не спрашивать подтверждение. |
| `--dry-run` | Только показать количество строк к удалению. |

### Примеры

```bash
# Предпросмотр и подтверждение
python akvorado_delete_period.py --from 202602170800 --to 202602170900 --table default.flows

# Только посмотреть объём
python akvorado_delete_period.py --from 202602170800 --to 202602170900 --table default.flows --dry-run

# Несколько таблиц
python akvorado_delete_period.py --from 202602170805 --to 202602170905 \
  --table default.flows --table default.flows_1m0s --table default.flows_5m0s --table default.flows_1h0m0s
```

Удаление из одной таблицы не затрагивает остальные. В ClickHouse выполняется асинхронно (мутации); прогресс — в `system.mutations`.
