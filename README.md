# akvorado-tools

Утилиты для работы с Akvorado (ClickHouse): сравнение перцентиля с Zabbix, только Akvorado, удаление данных за период.

ClickHouse работает в Docker-контейнере; порт HTTP (8123) примаплен с контейнера на хост. Подключение к ClickHouse — по SSH-туннелю: с локальной машины туннель пробрасывает порт хоста (8123) к себе, запросы идут в ClickHouse через этот туннель.

## Требования

- Python 3.6+
- `requests`
- SSH-доступ к хосту, где запущен Docker с ClickHouse (ключ задаётся через `--akvorado-key`)
- Для режима Zabbix / сравнения: `ZABBIX_URL`, `ZABBIX_TOKEN`

```bash
pip install -r requirements.txt
```

---

## 1. `zabbix_percentile.py` — перцентиль и сравнение

95% перцентиль по истории Zabbix за период; опционально сравнение с Akvorado; режимы «только Akvorado» и discover таблиц.

### Режимы работы

| Режим | Как включить | Обязательные опции |
|-------|--------------|---------------------|
| **Zabbix** | по умолчанию | `--host`, `--interface` или `--key`, `--from`, `--to` |
| **Zabbix + Akvorado** | `--akvorado` | то же + `--akvorado-host`; при необходимости `--akvorado-table`, `--akvorado-interval`, `--akvorado-exporter` |
| **Только Akvorado** | `--akvorado-only` | `--akvorado-host`, `--from`, `--to` и либо `--akvorado-boundary-only`, либо `--akvorado-in-if` + (`--host` или `--akvorado-exporter`) |
| **Discover по ExporterName/InIfName** | `--akvorado-discover` | `--akvorado-host`, `--akvorado-in-if`, и `--host` или `--akvorado-exporter` |
| **Discover таблиц (boundary)** | `--akvorado-discover-boundary` | только `--akvorado-host`; список таблиц с InIfBoundary=external, без фильтра по хосту/интерфейсу |
| **По всем таблицам** | `--akvorado-only --akvorado-boundary-only --akvorado-all-tables` | `--akvorado-host`, `--from`, `--to` — перцентиль по каждой таблице |

Полный список опций по разделам: `python3 zabbix_percentile.py --help`.

### Важные опции

- **`--akvorado-host`** — хост Akvorado для SSH. **Обязателен** при любом режиме с Akvorado (дефолта нет).
- **`--host`** — хост в Zabbix; для Akvorado по умолчанию ExporterName = `internet@<host>` (регистр в ClickHouse сохраняется).
- **`--interface`** — имя интерфейса (например `ae5`) или SNMP-индекс (например `635`). При имени ищется item по названию в Zabbix (например «Interface ae5(Beeline): Bits received»).
- **`--from`**, **`--to`** — период (YYYYMMDD или YYYYMMDDHHMM).
- **`--akvorado-exporter`** — ExporterName в ClickHouse. Если в UI отображается `internet@MSK-M9-MX204-1`, укажите именно так (регистр важен).
- **`--akvorado-in-if`** — InIfName в ClickHouse (например `ae5.0`).
- **`--akvorado-table`** — таблица (по умолчанию `flows`). Для длинных периодов: `default.flows_5m0s`.
- **`--akvorado-interval`** — окно агрегации в секундах: 60, 300 (5m), 3600 (1h). Для `flows_5m0s` укажите 300.

### Примеры

```bash
# Только Akvorado, InIfBoundary=external (без ExporterName/InIfName)
python3 zabbix_percentile.py --akvorado-only --akvorado-boundary-only \
  --akvorado-host msk-akvorado --from 20260201 --to 20260301

# Только Akvorado по конкретному интерфейсу (как в UI: InIfName + ExporterName)
python3 zabbix_percentile.py --akvorado-only --akvorado-host msk-akvorado \
  --from 20260201 --to 20260301 \
  --akvorado-exporter "internet@MSK-M9-MX204-1" --akvorado-in-if ae5.0 \
  --akvorado-table default.flows_5m0s --akvorado-interval 300

# Список таблиц с InIfBoundary=external (--host не используется)
python3 zabbix_percentile.py --akvorado-discover-boundary --akvorado-host msk-akvorado

# Таблицы, где есть данные по ExporterName/InIfName (если 0 записей — скрипт выведет примеры ExporterName из ClickHouse)
python3 zabbix_percentile.py --akvorado-discover --akvorado-host msk-akvorado \
  --akvorado-in-if ae5.0 --akvorado-exporter "internet@MSK-M9-MX204-1"

# Zabbix + сравнение с Akvorado (ZABBIX_URL, ZABBIX_TOKEN обязательны)
python3 zabbix_percentile.py --host msk-m9-mx204-1 --interface ae5 --direction in \
  --from 20260201 --to 20260301 --akvorado --akvorado-host msk-akvorado \
  --akvorado-exporter "internet@MSK-M9-MX204-1" --akvorado-table default.flows_5m0s --akvorado-interval 300
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
