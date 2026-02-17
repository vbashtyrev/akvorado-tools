#!/usr/bin/env python3
"""
Удаление данных за период в Akvorado (ClickHouse).
Сначала выводится, что и сколько будет удалено; затем запрос подтверждения (yes/no).
Использование:
  python akvorado_delete_period.py --from 202602170800 --to 202602170900 --table default.flows
  python akvorado_delete_period.py --from 20260201 --to 20260218 --table default.flows_5m0s --boundary-only --yes
"""

import argparse
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone

try:
    import requests as _requests
except ImportError:
    _requests = None


def parse_date(s):
    s = re.sub(r"[\s\-:]", "", str(s).strip())
    if len(s) == 8:
        dt = datetime.strptime(s, "%Y%m%d")
    elif len(s) == 12:
        dt = datetime.strptime(s, "%Y%m%d%H%M")
    elif len(s) == 14:
        dt = datetime.strptime(s, "%Y%m%d%H%M%S")
    else:
        raise ValueError("дата: YYYYMMDD, YYYYMMDDHHMM или YYYYMMDDHHMMSS (получено {} символов)".format(len(s)))
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp()), dt.strftime("%Y-%m-%d %H:%M:%S")


def _ch_post(url, sql, timeout=120):
    try:
        r = _requests.post(url, data=sql.encode("utf-8"), timeout=timeout, headers={"Content-Type": "text/plain; charset=utf-8"})
        r.raise_for_status()
        return r.text.strip(), None
    except Exception as e:
        return None, str(e)


def _table_sql_name(table_name):
    if "." in table_name and not table_name.startswith("."):
        parts = table_name.split(".", 1)
        return "`{}`.`{}`".format(parts[0].replace("`", "``"), parts[1].replace("`", "``"))
    return "`{}`".format(table_name.replace("`", "``"))


def count_in_period(url, table_name, from_ts, to_ts, boundary_only, use_utc=True):
    """Возврат (count, error)."""
    tbl = _table_sql_name(table_name)
    tz = ", 'UTC'" if use_utc else ""
    where = "TimeReceived >= toDateTime('{}'{}) AND TimeReceived < toDateTime('{}'{})".format(from_ts, tz, to_ts, tz)
    if boundary_only:
        where += " AND InIfBoundary = 'external'"
    sql = "SELECT count() FROM {} WHERE {} FORMAT TabSeparated".format(tbl, where)
    out, err = _ch_post(url, sql, timeout=300)
    if err:
        return None, err
    try:
        return int(out.split()[0]) if out else 0, None
    except (ValueError, IndexError):
        return None, "не удалось разобрать count: {}".format(out[:200])


def run_delete(url, table_name, from_ts, to_ts, boundary_only, use_utc=True):
    """Запуск ALTER TABLE ... DELETE WHERE ... Возврат (success, error)."""
    tbl = _table_sql_name(table_name)
    tz = ", 'UTC'" if use_utc else ""
    where = "TimeReceived >= toDateTime('{}'{}) AND TimeReceived < toDateTime('{}'{})".format(from_ts, tz, to_ts, tz)
    if boundary_only:
        where += " AND InIfBoundary = 'external'"
    sql = "ALTER TABLE {} DELETE WHERE {}".format(tbl, where)
    _, err = _ch_post(url, sql, timeout=60)
    if err:
        return False, err
    return True, None


def main():
    ap = argparse.ArgumentParser(description="Удаление данных за период в Akvorado (ClickHouse) с предпросмотром и подтверждением.")
    ap.add_argument("--from", dest="date_from", required=True, metavar="FROM", help="Начало периода: YYYYMMDD или YYYYMMDDHHMM")
    ap.add_argument("--to", dest="date_to", required=True, metavar="TO", help="Конец периода: YYYYMMDD или YYYYMMDDHHMM")
    ap.add_argument("--table", action="append", dest="tables", metavar="TABLE", help="Таблица (default.flows и т.д.). Можно указать несколько раз.")
    ap.add_argument("--boundary-only", action="store_true", help="Удалять только строки с InIfBoundary = 'external'")
    ap.add_argument("--akvorado-host", default="msk-akvorado", metavar="HOST", help="Хост Akvorado (SSH)")
    ap.add_argument("--akvorado-user", default="bvs", metavar="USER", help="Пользователь SSH")
    ap.add_argument("--akvorado-key", default="~/.ssh/proCloud", metavar="PATH", help="Путь к ключу SSH")
    ap.add_argument("--akvorado-port", type=int, default=18123, metavar="PORT", help="Локальный порт SSH-туннеля")
    ap.add_argument("--akvorado-tz-local", action="store_true", help="Время периода — локальное время сервера ClickHouse")
    ap.add_argument("--yes", action="store_true", help="Не спрашивать подтверждение")
    ap.add_argument("--dry-run", action="store_true", help="Только показать, что будет удалено; не выполнять удаление")
    args = ap.parse_args()

    if not args.tables:
        ap.error("Укажите хотя бы одну таблицу: --table default.flows")

    if _requests is None:
        print("Требуется requests: pip install requests", file=sys.stderr)
        return 1

    key_path = os.path.expanduser(args.akvorado_key)
    if not os.path.isfile(key_path):
        print("Ключ SSH не найден: {}".format(key_path), file=sys.stderr)
        return 1

    try:
        time_from, from_dt = parse_date(args.date_from)
        time_till, to_dt = parse_date(args.date_to)
    except ValueError as e:
        print("Период: {}".format(e), file=sys.stderr)
        return 1
    if time_from >= time_till:
        print("--from должно быть раньше --to", file=sys.stderr)
        return 1

    use_utc = not args.akvorado_tz_local
    boundary_only = args.boundary_only

    tunnel_cmd = [
        "ssh", "-N", "-o", "ExitOnForwardFailure=yes",
        "-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
        "-L", "{}:127.0.0.1:8123".format(args.akvorado_port),
        "-i", key_path, "{}@{}".format(args.akvorado_user, args.akvorado_host),
    ]
    proc = subprocess.Popen(tunnel_cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    time.sleep(1.5)
    if proc.poll() is not None:
        err = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
        print("SSH-туннель не поднялся: {}".format(err.strip() or "ssh завершился"), file=sys.stderr)
        return 1

    url = "http://127.0.0.1:{}/".format(args.akvorado_port)
    try:
        print("Период: {} — {}".format(from_dt, to_dt))
        if boundary_only:
            print("Фильтр: только InIfBoundary = 'external'")
        print("")
        print("Количество строк к удалению:")
        totals = []
        for tbl in args.tables:
            cnt, err = count_in_period(url, tbl, from_dt, to_dt, boundary_only, use_utc)
            if err:
                print("  {}: ошибка — {}".format(tbl, err))
                totals.append((tbl, None))
            else:
                print("  {}: {} записей".format(tbl, cnt))
                totals.append((tbl, cnt))
        total_count = sum(c for _, c in totals if c is not None)
        has_error = any(c is None for _, c in totals)
        if has_error:
            print("", file=sys.stderr)
            print("Не удалось получить count для всех таблиц. Удаление отменено.", file=sys.stderr)
            return 1
        if total_count == 0:
            print("")
            print("Нет записей за указанный период. Удаление не требуется.")
            return 0
        print("")
        print("Всего к удалению: {} записей в {} таблице(ах).".format(total_count, len(args.tables)))

        if args.dry_run:
            print("Режим --dry-run: удаление не выполняется.")
            return 0

        if not args.yes:
            print("")
            confirm = input("Подтвердить удаление? (yes/no): ").strip().lower()
            if confirm != "yes":
                print("Отменено.")
                return 0

        print("")
        for tbl in args.tables:
            ok, err = run_delete(url, tbl, from_dt, to_dt, boundary_only, use_utc)
            if err:
                print("  {}: ошибка — {}".format(tbl, err))
            else:
                print("  {}: мутация отправлена (ALTER TABLE ... DELETE). Ход в system.mutations.".format(tbl))
        print("")
        print("Готово. Удаление в ClickHouse выполняется асинхронно (mutations).")
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
    return 0


if __name__ == "__main__":
    sys.exit(main())
