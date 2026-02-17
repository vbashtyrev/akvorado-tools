#!/usr/bin/env python3
"""
95% перцентиль по истории Zabbix за период; сравнение с Akvorado (ClickHouse по SSH); режим только Akvorado (--akvorado-only).
Zabbix 7.0: ZABBIX_URL, ZABBIX_TOKEN.
"""

import argparse
import os
import re
import sys
from datetime import datetime, timezone

import subprocess
import time

try:
    import requests as _requests
except ImportError:
    _requests = None


def zabbix_request(url, token, method, params=None, debug=False):
    """Zabbix API 7 (JSON-RPC 2.0), Bearer. Возврат (result, None) или (None, error_msg)."""
    if _requests is None:
        return None, "для Zabbix нужен requests (pip install requests)"
    if params is None:
        params = {}
    if debug:
        print("Zabbix API: {} {}".format(method, params), file=sys.stderr)
    payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    headers = {"Content-Type": "application/json-rpc", "Authorization": "Bearer {}".format(token)}
    try:
        r = _requests.post(url, json=payload, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()
    except _requests.RequestException as e:
        return None, "запрос к Zabbix: {}".format(e)
    if "error" in data:
        err = data["error"]
        return None, "Zabbix API: {} ({})".format(err.get("data", err.get("message", "unknown")), err.get("code", ""))
    return data.get("result"), None


def validate_zabbix_token(url, token, debug=False):
    """Проверить токен: user.get (Zabbix 7)."""
    result, err = zabbix_request(url, token, "user.get", {"limit": 1}, debug=debug)
    if err:
        return False, err
    return True, None


def parse_date(s):
    """
    Парсинг даты и времени в Unix timestamp (UTC).
    Формат: YYYYMMDD или YYYYMMDDHHMM или YYYYMMDDHHMMSS (пробелы/дефисы/двоеточия игнорируются).
    Возврат (unix_ts, "YYYY-MM-DD HH:MM:SS").
    """
    s = re.sub(r"[\s\-:]", "", str(s).strip())
    if len(s) == 8:
        dt = datetime.strptime(s, "%Y%m%d")
    elif len(s) == 12:
        dt = datetime.strptime(s, "%Y%m%d%H%M")
    elif len(s) == 14:
        dt = datetime.strptime(s, "%Y%m%d%H%M%S")
    else:
        raise ValueError("дата/время: ожидается YYYYMMDD, YYYYMMDDHHMM или YYYYMMDDHHMMSS (получено {} символов)".format(len(s)))
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp()), dt.strftime("%Y-%m-%d %H:%M:%S")


def date_to_human(ymd):
    """YYYYMMDD или YYYYMMDDHHMM(SS) -> YYYY-MM-DD или YYYY-MM-DD HH:MM(:SS)."""
    s = re.sub(r"[\s\-:]", "", str(ymd).strip())
    if len(s) == 8:
        return "{}-{}-{}".format(s[0:4], s[4:6], s[6:8])
    if len(s) >= 12:
        return "{}-{}-{} {}:{}".format(s[0:4], s[4:6], s[6:8], s[8:10], s[10:12]) + ("" if len(s) <= 12 else ":{}".format(s[12:14]))
    return ymd


def human_bps(bps):
    """Биты в секунду в человекочитаемый вид (bps, Kbps, Mbps, Gbps)."""
    bps = float(bps)
    if bps >= 1e9:
        return "{:.2f} Gbps".format(bps / 1e9)
    if bps >= 1e6:
        return "{:.2f} Mbps".format(bps / 1e6)
    if bps >= 1e3:
        return "{:.2f} Kbps".format(bps / 1e3)
    return "{:.2f} bps".format(bps)


def build_item_key(interface, direction="in"):
    """Собрать ключ item'а по индексу интерфейса: 635 -> net.if.in[ifHCInOctets.635]."""
    # interface может быть "635" или "ifHCInOctets.635"
    idx = interface.lstrip("ifHCInOctets.").strip()
    if direction == "out":
        return "net.if.out[ifHCOutOctets.{}]".format(idx)
    return "net.if.in[ifHCInOctets.{}]".format(idx)


def interface_name_from_item_name(name):
    """Из названия item вида 'Interface ae5(Beeline): Bits received' извлечь имя интерфейса ae5."""
    if not name:
        return None
    # "Interface ae5(" или "Interface Ethernet51/1(Uplink..."
    m = re.search(r"Interface\s+([^\s(:(]+)", name, re.IGNORECASE)
    return m.group(1).strip() if m else None


def get_host_and_item(url, token, host_name, item_key, debug=False):
    """Найти hostid по имени и itemid по ключу. Возврат (hostid, itemid, item_name, err)."""
    result, err = zabbix_request(url, token, "host.get", {
        "output": ["hostid", "host"],
        "filter": {"host": [host_name]},
    }, debug=debug)
    if err:
        return None, None, None, err
    if not result:
        # попробовать по visible name (name)
        result, err = zabbix_request(url, token, "host.get", {
            "output": ["hostid", "host", "name"],
            "search": {"name": host_name},
            "searchByAny": True,
        }, debug=debug)
        if err or not result:
            return None, None, None, "хост не найден: {}".format(host_name)
    hostid = result[0]["hostid"]

    result, err = zabbix_request(url, token, "item.get", {
        "output": ["itemid", "key_", "name"],
        "hostids": [hostid],
        "search": {"key_": item_key},
        "searchByAny": False,
    }, debug=debug)
    if err:
        return hostid, None, None, err
    if not result:
        return hostid, None, None, "item не найден: key_={}".format(item_key)
    item = result[0]
    return hostid, item["itemid"], item.get("name") or "", None


def fetch_history(url, token, itemid, time_from, time_till, history_type=3, limit=100000, debug=False):
    """
    Собрать всю историю за период (пагинация по time_from). Возврат (values, timestamps_unix), error.
    timestamps_unix — список clock для каждой точки (для проверки пропусков).
    """
    all_values = []
    all_clocks = []
    current_from = time_from
    while current_from < time_till:
        result, err = zabbix_request(url, token, "history.get", {
            "itemids": [itemid],
            "time_from": current_from,
            "time_till": time_till,
            "output": ["itemid", "clock", "value"],
            "history": history_type,
            "sortfield": "clock",
            "sortorder": "ASC",
            "limit": limit,
        }, debug=debug)
        if err:
            return None, None, err
        if not result:
            break
        for row in result:
            try:
                v = float(row["value"])
                all_values.append(v)
                all_clocks.append(int(row["clock"]))
            except (TypeError, ValueError):
                continue
        last_clock = int(result[-1]["clock"])
        if last_clock >= time_till - 1:
            break
        current_from = last_clock + 1
        if debug and all_values:
            print("  history.get: получено {} записей, всего значений {}".format(len(result), len(all_values)), file=sys.stderr)
    return all_values, all_clocks, None


def percentile_sorted(values, p):
    """Перцентиль p (0..100) по отсортированному списку."""
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1 if f + 1 < len(s) else f
    return s[f] + (k - f) * (s[c] - s[f]) if f != c else s[f]


def find_gaps(timestamps_unix, period_start_unix, period_end_unix, expected_interval_sec, tolerance_sec=None):
    """
    Найти пропуски в данных: периоды, когда данных нет.
    expected_interval_sec — ожидаемый шаг между точками (60 для Zabbix 1min, 300 для Akvorado 5m).
    tolerance_sec — если разрыв между соседними точками > expected_interval_sec + tolerance, считаем пропуск. По умолчанию 0.5 * expected.
    Возврат: список (gap_start_unix, gap_end_unix).
    """
    if tolerance_sec is None:
        tolerance_sec = max(1, expected_interval_sec // 2)
    threshold = expected_interval_sec + tolerance_sec
    ts = sorted(set(timestamps_unix))
    gaps = []
    if not ts:
        gaps.append((period_start_unix, period_end_unix))
        return gaps
    if ts[0] - period_start_unix > threshold:
        gaps.append((period_start_unix, ts[0]))
    for i in range(len(ts) - 1):
        if ts[i + 1] - ts[i] > threshold:
            gaps.append((ts[i], ts[i + 1]))
    if period_end_unix - ts[-1] > threshold:
        gaps.append((ts[-1], period_end_unix))
    return gaps


def format_ts_unix(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def fetch_akvorado_bps(ssh_host, ssh_user, key_path, exporter_name, in_if_name, date_from_str, date_to_str, local_port=18123, in_if_boundary="external", boundary_only=False, use_utc=True, table_name="flows", interval_sec=60, debug=False):
    """
    Проброс порта ClickHouse по SSH-туннелю и запрос с локальной машины. Всегда Bytes*SamplingRate.
    boundary_only: только фильтр InIfBoundary = 'external' (без ExporterName/InIfName), как в UI «InIfBoundary = external».
    """
    if _requests is None:
        return None, None, "для --akvorado нужен requests (pip install requests)"
    key_path = os.path.expanduser(key_path)
    if not os.path.isfile(key_path):
        return None, None, "ключ SSH не найден: {}".format(key_path)
    from_dt = date_from_str if " " in date_from_str else "{} 00:00:00".format(date_from_str)
    to_dt = date_to_str if " " in date_to_str else "{} 00:00:00".format(date_to_str)
    def esc(s):
        return s.replace("\\", "\\\\").replace("'", "''")
    exporter_esc = esc(exporter_name or "")
    in_if_esc = esc(in_if_name or "")
    tz_suffix = ", 'UTC'" if use_utc else ""
    if boundary_only:
        where_extra = "WHERE InIfBoundary = 'external' "
    else:
        where_extra = "WHERE ExporterName = '{exporter}' AND InIfName = '{inif}'".format(exporter=exporter_esc, inif=in_if_esc)
        if in_if_boundary:
            where_extra += " AND InIfBoundary = 'external' "
        else:
            where_extra += " "
    # Полное имя таблицы
    if "." in table_name and not table_name.startswith("."):
        parts = table_name.split(".", 1)
        tbl_sql = "`{}`.`{}`".format(parts[0].replace("`", "``"), parts[1].replace("`", "``"))
    else:
        tbl_sql = "`{}`".format(table_name.replace("`", "``"))
    bytes_expr = "Bytes * coalesce(SamplingRate, 1)"
    if interval_sec != 60:
        sql = (
            "SELECT TimeReceived AS minute, sum({bytes}) * 8 / {interval} AS bps "
            "FROM {tbl} "
            "{where_extra}"
            "AND TimeReceived >= toDateTime('{from_ts}'{tz}) AND TimeReceived < toDateTime('{to_ts}'{tz}) "
            "GROUP BY TimeReceived ORDER BY TimeReceived FORMAT TabSeparated"
        ).format(bytes=bytes_expr, tbl=tbl_sql, interval=interval_sec, where_extra=where_extra, from_ts=from_dt, to_ts=to_dt, tz=tz_suffix)
    else:
        sql = (
            "SELECT toStartOfMinute(TimeReceived) AS minute, sum({bytes}) * 8 / 60 AS bps "
            "FROM {tbl} "
            "{where_extra}"
            "AND TimeReceived >= toDateTime('{from_ts}'{tz}) AND TimeReceived < toDateTime('{to_ts}'{tz}) "
            "GROUP BY minute ORDER BY minute FORMAT TabSeparated"
        ).format(tbl=tbl_sql, bytes=bytes_expr, where_extra=where_extra, from_ts=from_dt, to_ts=to_dt, tz=tz_suffix)
    if debug:
        print("Akvorado SQL: {}".format(sql), file=sys.stderr)
    # Туннель: -L local_port:127.0.0.1:8123 (на удалённом хосте 8123 уже проброшен с Docker)
    tunnel_cmd = [
        "ssh", "-N", "-o", "ExitOnForwardFailure=yes",
        "-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
        "-L", "{}:127.0.0.1:8123".format(local_port),
        "-i", key_path, "{}@{}".format(ssh_user, ssh_host),
    ]
    proc = None
    diag_msg = None
    try:
        proc = subprocess.Popen(tunnel_cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        time.sleep(1.5)
        if proc.poll() is not None:
            err = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
            return None, None, "SSH-туннель не поднялся: {}".format(err.strip() or "ssh завершился")
        url = "http://127.0.0.1:{}/".format(local_port)
        r = _requests.post(url, data=sql.encode("utf-8"), timeout=120, headers={"Content-Type": "text/plain; charset=utf-8"})
        r.raise_for_status()
        out = r.text
        values = []
        timestamps_unix = []
        for line in out.strip().splitlines():
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) >= 2:
                try:
                    ts_str = parts[0].strip()
                    dt = datetime.strptime(ts_str[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    timestamps_unix.append(int(dt.timestamp()))
                    values.append(float(parts[1]))
                except (ValueError, TypeError):
                    continue
        # Если за период пусто — диагностика без фильтра по времени (туннель ещё открыт)
        if len(values) == 0:
            if boundary_only:
                diag_where = "InIfBoundary = 'external'"
            else:
                diag_where = "ExporterName = '{exporter}' AND InIfName = '{inif}'".format(exporter=exporter_esc, inif=in_if_esc)
                if in_if_boundary:
                    diag_where += " AND InIfBoundary = 'external'"
            diag_sql = (
                "SELECT count() AS c, toString(min(TimeReceived)) AS tmin, toString(max(TimeReceived)) AS tmax "
                "FROM {tbl} WHERE {where} FORMAT TabSeparated"
            ).format(tbl=tbl_sql, where=diag_where)
            try:
                r2 = _requests.post(url, data=diag_sql.encode("utf-8"), timeout=30, headers={"Content-Type": "text/plain; charset=utf-8"})
                if r2.status_code == 200 and r2.text.strip():
                    row = r2.text.strip().split("\t")
                    if len(row) >= 3:
                        c, tmin, tmax = row[0], row[1], row[2]
                        # Данные в таблице только в диапазоне [tmin, tmax]; запрошенный период может не пересекаться
                        diag_msg = (
                            "в таблице записей={}, данные только за период min(TimeReceived)={}, max(TimeReceived)={}. "
                            "Запрошенный период ({} — {}) не пересекается с этим диапазоном. "
                            "Укажите --from / --to в пределах данных, например для февраля: --from 20260202 --to 20260218."
                        ).format(c, tmin, tmax, from_dt[:10], to_dt[:10])
                    else:
                        diag_msg = "ответ диагностики: " + r2.text.strip()[:200]
                else:
                    diag_msg = "диагностический запрос вернул код {} или пусто.".format(r2.status_code)
            except Exception as e:
                diag_msg = "диагностика не удалась: {}".format(e)
    except FileNotFoundError:
        return None, None, "ssh не найден в PATH"
    except _requests.RequestException as e:
        return None, None, "ClickHouse HTTP: {}".format(e)
    except Exception as e:
        return None, None, "туннель/запрос: {}".format(e)
    finally:
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
    if not values and diag_msg:
        label = "InIfBoundary=external" if boundary_only else "ExporterName={}, InIfName={}".format(exporter_name, in_if_name)
        return None, None, "за период данных нет ({}). {}".format(label, diag_msg)
    if not values:
        label = "InIfBoundary=external" if boundary_only else "ExporterName={}, InIfName={}".format(exporter_name, in_if_name)
        return None, None, "за период данных нет ({}).".format(label)
    return values, timestamps_unix, None


def _ch_post(url, sql, timeout=60):
    """Один запрос к ClickHouse HTTP; возврат (text или None, error)."""
    try:
        r = _requests.post(url, data=sql.encode("utf-8"), timeout=timeout, headers={"Content-Type": "text/plain; charset=utf-8"})
        r.raise_for_status()
        return r.text, None
    except Exception as e:
        return None, str(e)


def discover_akvorado_tables(ssh_host, ssh_user, key_path, exporter_name, in_if_name, local_port=18123):
    """
    Найти в ClickHouse все таблицы, где есть данные по ExporterName/InIfName, и диапазон времени (без фильтра InIfBoundary).
    Возврат (list of dict: database, table, time_col, count, min_t, max_t), error).
    """
    if _requests is None:
        return None, "нужен requests"
    key_path = os.path.expanduser(key_path)
    if not os.path.isfile(key_path):
        return None, "ключ SSH не найден: {}".format(key_path)
    def esc(s):
        return s.replace("\\", "\\\\").replace("'", "''")
    exporter_esc = esc(exporter_name)
    in_if_esc = esc(in_if_name)

    tunnel_cmd = [
        "ssh", "-N", "-o", "ExitOnForwardFailure=yes",
        "-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
        "-L", "{}:127.0.0.1:8123".format(local_port),
        "-i", key_path, "{}@{}".format(ssh_user, ssh_host),
    ]
    proc = subprocess.Popen(tunnel_cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    time.sleep(1.5)
    if proc.poll() is not None:
        err = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
        return None, "SSH-туннель не поднялся: {}".format(err.strip() or "ssh завершился")
    url = "http://127.0.0.1:{}/".format(local_port)
    results = []
    try:
        # Все таблицы (database, name) кроме системных
        out, err = _ch_post(url, "SELECT database, name FROM system.tables WHERE database NOT IN ('system','INFORMATION_SCHEMA','information_schema') FORMAT TabSeparated", timeout=30)
        if err:
            return None, err
        tables = []
        for line in out.strip().splitlines():
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) >= 2:
                tables.append((parts[0], parts[1]))

        # Возможные имена колонки времени
        time_candidates = ["TimeReceived", "TimeFlow", "TimeInserted", "EventTime", "Timestamp"]
        for db, table in tables:
            full_name = "`{}`.`{}`".format(db.replace("`", "``"), table.replace("`", "``"))
            out, err = _ch_post(url, "SELECT name FROM system.columns WHERE database = '{}' AND table = '{}' FORMAT TabSeparated".format(db.replace("'", "''"), table.replace("'", "''")), timeout=15)
            if err or not out.strip():
                continue
            cols = [l.strip().lower() for l in out.strip().splitlines()]
            if "exportername" not in cols or "inifname" not in cols:
                continue
            time_col = None
            for tc in time_candidates:
                if tc.lower() in cols:
                    time_col = tc
                    break
            if not time_col:
                continue
            # Проверяем наличие Bytes (или Packets) для битрейта
            if "bytes" not in cols:
                continue
            probe_sql = (
                "SELECT count() AS c, toString(min({tc})) AS tmin, toString(max({tc})) AS tmax FROM {tbl} "
                "WHERE ExporterName = '{exporter}' AND InIfName = '{inif}' FORMAT TabSeparated"
            ).format(tc=time_col, tbl=full_name, exporter=exporter_esc, inif=in_if_esc)
            out, err = _ch_post(url, probe_sql, timeout=60)
            if err:
                results.append({"database": db, "table": table, "time_col": time_col, "count": None, "min_t": None, "max_t": None, "error": err})
                continue
            row = out.strip().split("\t") if out.strip() else []
            if len(row) >= 3:
                try:
                    c = int(row[0])
                    results.append({"database": db, "table": table, "time_col": time_col, "count": c, "min_t": row[1], "max_t": row[2], "error": None})
                except ValueError:
                    results.append({"database": db, "table": table, "time_col": time_col, "count": None, "min_t": row[1] if len(row)>1 else None, "max_t": row[2] if len(row)>2 else None, "error": "parse"})
            else:
                results.append({"database": db, "table": table, "time_col": time_col, "count": 0, "min_t": None, "max_t": None, "error": None})
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
    return results, None


def discover_akvorado_tables_boundary(ssh_host, ssh_user, key_path, local_port=18123):
    """
    Найти все таблицы с колонками InIfBoundary, Bytes, TimeReceived; для каждой — count/min/max по InIfBoundary='external'.
    Возврат (list of dict: database, table, time_col, count, min_t, max_t), error).
    """
    if _requests is None:
        return None, "нужен requests"
    key_path = os.path.expanduser(key_path)
    if not os.path.isfile(key_path):
        return None, "ключ SSH не найден: {}".format(key_path)
    tunnel_cmd = [
        "ssh", "-N", "-o", "ExitOnForwardFailure=yes",
        "-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
        "-L", "{}:127.0.0.1:8123".format(local_port),
        "-i", key_path, "{}@{}".format(ssh_user, ssh_host),
    ]
    proc = subprocess.Popen(tunnel_cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    time.sleep(1.5)
    if proc.poll() is not None:
        err = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
        return None, "SSH-туннель не поднялся: {}".format(err.strip() or "ssh завершился")
    url = "http://127.0.0.1:{}/".format(local_port)
    results = []
    try:
        out, err = _ch_post(url, "SELECT database, name FROM system.tables WHERE database NOT IN ('system','INFORMATION_SCHEMA','information_schema') FORMAT TabSeparated", timeout=30)
        if err:
            return None, err
        tables = []
        for line in out.strip().splitlines():
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) >= 2:
                tables.append((parts[0], parts[1]))
        time_candidates = ["TimeReceived", "TimeFlow", "TimeInserted", "EventTime", "Timestamp"]
        for db, table in tables:
            full_name = "`{}`.`{}`".format(db.replace("`", "``"), table.replace("`", "``"))
            out, err = _ch_post(url, "SELECT name FROM system.columns WHERE database = '{}' AND table = '{}' FORMAT TabSeparated".format(db.replace("'", "''"), table.replace("'", "''")), timeout=15)
            if err or not out.strip():
                continue
            cols = [l.strip().lower() for l in out.strip().splitlines()]
            if "inifboundary" not in cols or "bytes" not in cols:
                continue
            time_col = None
            for tc in time_candidates:
                if tc.lower() in cols:
                    time_col = tc
                    break
            if not time_col:
                continue
            probe_sql = (
                "SELECT count() AS c, toString(min({tc})) AS tmin, toString(max({tc})) AS tmax FROM {tbl} "
                "WHERE InIfBoundary = 'external' FORMAT TabSeparated"
            ).format(tc=time_col, tbl=full_name)
            out, err = _ch_post(url, probe_sql, timeout=120)
            if err:
                results.append({"database": db, "table": table, "time_col": time_col, "count": None, "min_t": None, "max_t": None, "error": err})
                continue
            row = out.strip().split("\t") if out.strip() else []
            if len(row) >= 3:
                try:
                    c = int(row[0])
                    results.append({"database": db, "table": table, "time_col": time_col, "count": c, "min_t": row[1], "max_t": row[2], "error": None})
                except ValueError:
                    results.append({"database": db, "table": table, "time_col": time_col, "count": None, "min_t": row[1] if len(row) > 1 else None, "max_t": row[2] if len(row) > 2 else None, "error": "parse"})
            else:
                results.append({"database": db, "table": table, "time_col": time_col, "count": 0, "min_t": None, "max_t": None, "error": None})
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
    return results, None


def _interval_from_table_name(table_name):
    """По имени таблицы подобрать интервал в секундах (60/300/3600)."""
    t = (table_name or "").lower()
    if "5m" in t or "_5m0s" in t:
        return 300
    if "1h" in t or "1h0m" in t:
        return 3600
    return 60


def main():
    ap = argparse.ArgumentParser(
        description="95%% перцентиль по истории item'а Zabbix за период (ZABBIX_URL, ZABBIX_TOKEN)."
    )
    ap.add_argument("--host", metavar="HOST", help="Имя хоста в Zabbix (обязателен без --akvorado-only)")
    ap.add_argument("--interface", metavar="INDEX", help="Индекс интерфейса (например 635). Строит ключ net.if.in[ifHCInOctets.INDEX]")
    ap.add_argument("--direction", choices=("in", "out"), default="in", help="Направление: in (приём) или out (передача)")
    ap.add_argument("--key", metavar="KEY", help="Ключ item'а целиком (если задан, --interface и --direction игнорируются)")
    ap.add_argument("--from", dest="date_from", metavar="FROM", help="Начало периода: YYYYMMDD или YYYYMMDDHHMM или YYYYMMDDHHMMSS (например 20260101 или 202601011200)")
    ap.add_argument("--to", dest="date_to", metavar="TO", help="Конец периода: YYYYMMDD или YYYYMMDDHHMM или YYYYMMDDHHMMSS (например 20260201)")
    ap.add_argument("--percentile", type=float, default=95.0, help="Перцентиль (по умолчанию 95)")
    ap.add_argument("--akvorado", action="store_true", help="Сравнить с Akvorado (ClickHouse по SSH)")
    ap.add_argument("--akvorado-only", action="store_true", help="Только Akvorado: запрос и вывод без Zabbix (нужны --from, --to и --akvorado-boundary-only или --akvorado-in-if)")
    ap.add_argument("--akvorado-host", default="msk-akvorado", metavar="HOST", help="Хост Akvorado (SSH)")
    ap.add_argument("--akvorado-user", default="bvs", metavar="USER", help="Пользователь SSH")
    ap.add_argument("--akvorado-key", default="~/.ssh/proCloud", metavar="PATH", help="Путь к ключу SSH")
    ap.add_argument("--akvorado-exporter", metavar="NAME", help="ExporterName в ClickHouse (по умолчанию internet@HOST)")
    ap.add_argument("--akvorado-in-if", metavar="NAME", help="InIfName в ClickHouse (по умолчанию INTERFACE.0, например ae5.0)")
    ap.add_argument("--akvorado-port", type=int, default=18123, metavar="PORT", help="Локальный порт для SSH-туннеля к ClickHouse (по умолчанию 18123)")
    ap.add_argument("--akvorado-no-boundary", action="store_true", help="Не фильтровать по InIfBoundary (по умолчанию InIfBoundary = 'external', как в UI)")
    ap.add_argument("--akvorado-boundary-only", action="store_true", help="Только InIfBoundary = external (без ExporterName/InIfName), как в UI «InIfBoundary = external»")
    ap.add_argument("--akvorado-tz-local", action="store_true", help="Время периода — локальное время сервера ClickHouse (иначе UTC)")
    ap.add_argument("--akvorado-discover", action="store_true", help="Найти таблицы ClickHouse с данными по ExporterName/InIfName (нужны --host и --akvorado-in-if)")
    ap.add_argument("--akvorado-discover-boundary", action="store_true", help="Найти все таблицы с InIfBoundary=external и вывести по каждой: записей, min/max TimeReceived")
    ap.add_argument("--akvorado-all-tables", action="store_true", help="С --akvorado-only --akvorado-boundary-only: запрос по каждой таблице и вывод перцентиля по каждой")
    ap.add_argument("--akvorado-table", metavar="DB.TABLE", help="Таблица ClickHouse (например default.flows_5m0s для данных за январь)")
    ap.add_argument("--akvorado-interval", type=int, default=60, metavar="SEC", help="Окно агрегации таблицы в секундах: 60 (1min), 300 (5m), 3600 (1h). Для flows_5m0s укажите 300, для flows_1h0m0s — 3600")
    ap.add_argument("--check-gaps", action="store_true", help="Проверить непрерывность данных за период; вывести все пропуски (когда данных нет)")
    ap.add_argument("--check-gaps-interval", type=int, default=180, metavar="SEC", help="Ожидаемый интервал опроса Zabbix в секундах (по умолчанию 180 = 3 мин)")
    ap.add_argument("--debug", action="store_true", help="Отладочный вывод")
    args = ap.parse_args()

    if not args.akvorado_only and not args.akvorado_discover_boundary and not args.host:
        ap.error("Укажите --host (или используйте --akvorado-only / --akvorado-discover-boundary)")

    # Режим «только Akvorado»: без Zabbix, только запрос в ClickHouse и вывод
    if args.akvorado_only:
        if not args.date_from or not args.date_to:
            ap.error("Для --akvorado-only укажите --from и --to")
        if not args.akvorado_boundary_only and not args.akvorado_in_if:
            ap.error("Для --akvorado-only укажите --akvorado-boundary-only или --akvorado-in-if")
        if not args.akvorado_boundary_only and not args.akvorado_exporter and not args.host:
            ap.error("Для --akvorado-only без --akvorado-boundary-only укажите --host или --akvorado-exporter")
        try:
            time_from, from_dt = parse_date(args.date_from)
            time_till, to_dt = parse_date(args.date_to)
        except ValueError as e:
            print("Период: {}".format(e), file=sys.stderr)
            return 1
        if time_from >= time_till:
            print("--from должно быть раньше --to", file=sys.stderr)
            return 1
        period_human = "{} — {}".format(from_dt, to_dt)
        boundary_only = bool(args.akvorado_boundary_only)
        if boundary_only:
            akvorado_exporter = ""
            akvorado_in_if = ""
        else:
            akvorado_exporter = args.akvorado_exporter if args.akvorado_exporter else ("internet@" + args.host)
            akvorado_in_if = args.akvorado_in_if
        akvorado_boundary = None if args.akvorado_no_boundary else "external"

        # Режим «по всем таблицам»: discover по boundary, затем запрос и вывод по каждой таблице
        if boundary_only and args.akvorado_all_tables:
            found, err = discover_akvorado_tables_boundary(
                args.akvorado_host, args.akvorado_user, args.akvorado_key, local_port=args.akvorado_port,
            )
            if err:
                print("Ошибка: {}".format(err), file=sys.stderr)
                return 1
            if not found:
                print("Нет таблиц с InIfBoundary=external.", file=sys.stderr)
                return 1
            for r in sorted(found, key=lambda x: (x.get("database") or "", x.get("table") or "")):
                tbl_name = "{}.{}".format(r["database"], r["table"])
                interval_sec = _interval_from_table_name(r.get("table"))
                akv_bps_list, akv_ts, akv_err = fetch_akvorado_bps(
                    args.akvorado_host, args.akvorado_user, args.akvorado_key,
                    "", "",
                    from_dt, to_dt,
                    local_port=args.akvorado_port, in_if_boundary="external",
                    boundary_only=True,
                    use_utc=not args.akvorado_tz_local, table_name=tbl_name,
                    interval_sec=interval_sec, debug=args.debug,
                )
                print("")
                print("--- {} ---".format(tbl_name))
                if akv_err:
                    print("  ошибка: {}".format(akv_err))
                elif not akv_bps_list:
                    print("  за период данных нет")
                else:
                    p_akv = percentile_sorted(akv_bps_list, args.percentile)
                    n_akv = len(akv_bps_list)
                    print("  InIfBoundary:     external")
                    print("  Период:           {}".format(period_human))
                    print("  Точек (бакетов):  n = {}".format(n_akv))
                    print("  Перцентиль {:.0f}%: {}  ({})".format(args.percentile, human_bps(p_akv), p_akv))
            return 0

        akvorado_table = (args.akvorado_table or "flows").strip()
        akv_bps_list, akv_ts, akv_err = fetch_akvorado_bps(
            args.akvorado_host, args.akvorado_user, args.akvorado_key,
            akvorado_exporter, akvorado_in_if,
            from_dt, to_dt,
            local_port=args.akvorado_port, in_if_boundary=akvorado_boundary,
            boundary_only=boundary_only,
            use_utc=not args.akvorado_tz_local, table_name=akvorado_table,
            interval_sec=args.akvorado_interval, debug=args.debug,
        )
        if akv_err:
            print("Akvorado: {}".format(akv_err), file=sys.stderr)
            return 1
        if not akv_bps_list:
            if boundary_only:
                print("За период данных нет (InIfBoundary=external).", file=sys.stderr)
            else:
                print("За период данных нет (ExporterName={}, InIfName={}).".format(akvorado_exporter, akvorado_in_if), file=sys.stderr)
            return 1
        p_akv = percentile_sorted(akv_bps_list, args.percentile)
        n_akv = len(akv_bps_list)
        print("--- Akvorado (ClickHouse, flow, sample учтён) ---")
        if boundary_only:
            print("InIfBoundary:     external")
        else:
            print("ExporterName:     {}".format(akvorado_exporter))
            print("InIfName:         {}".format(akvorado_in_if))
        print("Период:           {}".format(period_human))
        print("Точек (бакетов):  n = {}".format(n_akv))
        print("Перцентиль {:.0f}%: {}  ({})".format(args.percentile, human_bps(p_akv), p_akv))
        if args.check_gaps and akv_ts:
            akv_gaps = find_gaps(akv_ts, time_from, time_till, args.akvorado_interval, tolerance_sec=max(1, args.akvorado_interval // 2))
            print("")
            print("Пропуски (ожидаемый шаг {} с):".format(args.akvorado_interval))
            if not akv_gaps:
                print("  данные за период непрерывны.")
            else:
                for g_start, g_end in akv_gaps:
                    print("  пропуск: {} — {}".format(format_ts_unix(g_start), format_ts_unix(g_end)))
        return 0

    # Просмотр всех таблиц по InIfBoundary=external (записей, min/max по каждой)
    if args.akvorado_discover_boundary:
        found, err = discover_akvorado_tables_boundary(
            args.akvorado_host, args.akvorado_user, args.akvorado_key, local_port=args.akvorado_port,
        )
        if err:
            print("Ошибка: {}".format(err), file=sys.stderr)
            return 1
        found.sort(key=lambda r: (r.get("database") or "", r.get("table") or ""))
        print("Таблицы с InIfBoundary = 'external':")
        print("")
        for r in found:
            tbl = "{}.{}".format(r["database"], r["table"])
            if r.get("error"):
                print("  {}\tошибка: {}".format(tbl, r["error"]))
            else:
                c = r.get("count") if r.get("count") is not None else 0
                print("  {}\tзаписей={}  min(TimeReceived)={}  max(TimeReceived)={}".format(
                    tbl, c, r.get("min_t") or "—", r.get("max_t") or "—"))
        if not found:
            print("  (нет таблиц с колонками InIfBoundary, Bytes, TimeReceived)")
        return 0

    # Режим поиска таблиц: не нужны Zabbix и период
    if args.akvorado_discover:
        if not args.akvorado_in_if:
            ap.error("Для --akvorado-discover укажите --akvorado-in-if (например ae5.0)")
        if not args.host and not args.akvorado_exporter:
            ap.error("Для --akvorado-discover укажите --host или --akvorado-exporter")
        exporter = args.akvorado_exporter if args.akvorado_exporter else ("internet@" + args.host)
        found, err = discover_akvorado_tables(
            args.akvorado_host, args.akvorado_user, args.akvorado_key,
            exporter, args.akvorado_in_if, local_port=args.akvorado_port,
        )
        if err:
            print("Ошибка: {}".format(err), file=sys.stderr)
            return 1
        # Сначала таблицы с данными, по возрастанию min_t (где раньше всего есть данные)
        found.sort(key=lambda r: (0 if (r.get("count") or 0) > 0 else 1, r.get("min_t") or "z"))
        print("Таблицы с ExporterName={}, InIfName={}:".format(exporter, args.akvorado_in_if))
        print("")
        for r in found:
            if r.get("error"):
                print("  {}.\t{}\tошибка: {}".format(r["database"], r["table"], r["error"]))
            else:
                c = r.get("count") if r.get("count") is not None else 0
                print("  {}.\t{}\t(time: {})  записей={}  min={}  max={}".format(
                    r["database"], r["table"], r["time_col"], c, r.get("min_t") or "—", r.get("max_t") or "—"))
        if not found:
            print("  (нет подходящих таблиц с колонками ExporterName, InIfName, Bytes, TimeReceived/TimeFlow)")
        else:
            print("")
            print("Для данных за январь используйте агрегированные таблицы (flows_5m0s или flows_1h0m0s):")
            print("  --akvorado-table default.flows_5m0s --akvorado-interval 300")
            print("  или  --akvorado-table default.flows_1h0m0s --akvorado-interval 3600")
        return 0

    if not args.key and not args.interface:
        ap.error("Укажите --key или --interface")
    if not args.date_from or not args.date_to:
        ap.error("Укажите --from и --to (период)")
    item_key = args.key if args.key else build_item_key(args.interface, args.direction)

    url = os.environ.get("ZABBIX_URL", "").rstrip("/")
    token = os.environ.get("ZABBIX_TOKEN", "")
    if not url or not token:
        print("Задайте ZABBIX_URL и ZABBIX_TOKEN", file=sys.stderr)
        return 1
    if not url.endswith("api_jsonrpc.php"):
        url = url.rstrip("/") + "/api_jsonrpc.php"

    valid, err = validate_zabbix_token(url, token, debug=args.debug)
    if not valid:
        print("Zabbix: {}".format(err), file=sys.stderr)
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

    hostid, itemid, item_name, err = get_host_and_item(url, token, args.host, item_key, debug=args.debug)
    if err:
        print(err, file=sys.stderr)
        return 1
    interface_label = interface_name_from_item_name(item_name) if item_name else None
    if args.debug:
        print("hostid={}, itemid={}, item name={}".format(hostid, itemid, item_name), file=sys.stderr)

    # Сначала пробуем history=3 (numeric unsigned), затем 0 (float)
    values, clocks, err = fetch_history(url, token, itemid, time_from, time_till, history_type=3, debug=args.debug)
    if err:
        print("history.get: {}".format(err), file=sys.stderr)
        return 1
    if not values:
        values, clocks, err = fetch_history(url, token, itemid, time_from, time_till, history_type=0, debug=args.debug)
        if err:
            print("history.get: {}".format(err), file=sys.stderr)
            return 1

    if not values:
        print("За период {} — {} данных нет".format(from_dt, to_dt), file=sys.stderr)
        return 1

    p_val = percentile_sorted(values, args.percentile)
    period_human = "{} — {}".format(from_dt, to_dt)
    # Значение item'а — обычно bps (change per second * 8)
    p_human = human_bps(p_val)

    n = len(values)
    s = sorted(values)
    k = (n - 1) * (args.percentile / 100.0)
    f = int(k)
    c = f + 1 if f + 1 < n else f

    print("Хост:              {}".format(args.host))
    print("Интерфейс:        {}".format(interface_label if interface_label else "—"))
    if item_name:
        print("Item (Zabbix):     {}".format(item_name))
    print("Ключ item:        {}".format(item_key))
    print("Период:           {}".format(period_human))
    print("Точек в выборке:  n = {}".format(n))
    print("Перцентиль {:.0f}%: {}  ({})".format(args.percentile, p_human, p_val))
    print("Формула:          k = (n−1)·p/100 = ({}−1)·{}/100 = {:.4f}; "
          "P = v[⌊k⌋] + (k−⌊k⌋)·(v[⌊k⌋+1]−v[⌊k⌋]) = {} + ({:.4f})·({}−{}) = {:.4f}".format(
          n, args.percentile, k, s[f], k - f, s[c], s[f], p_val))
    print("Что за формула:   Значения за период сортируются по возрастанию (v[0] ≤ v[1] ≤ … ≤ v[n−1]). "
          "Позиция p%-перцентиля: k = (n−1)·p/100. Итог P получается линейной интерполяцией между "
          "v[⌊k⌋] и v[⌊k⌋+1], чтобы ровно p% выборки было ниже P.")

    if args.check_gaps:
        interval = args.check_gaps_interval
        tolerance = max(1, interval // 2)
        zabbix_gaps = find_gaps(clocks, time_from, time_till, expected_interval_sec=interval, tolerance_sec=tolerance)
        print("")
        print("--- Проверка непрерывности данных (--check-gaps) ---")
        print("Zabbix (ожидаемый шаг {} с):".format(interval))
        if not zabbix_gaps:
            print("  данные за период непрерывны.")
        else:
            for g_start, g_end in zabbix_gaps:
                print("  пропуск: {} — {}".format(format_ts_unix(g_start), format_ts_unix(g_end)))

    if args.akvorado:
        boundary_only = bool(args.akvorado_boundary_only)
        if boundary_only:
            akvorado_exporter = ""
            akvorado_in_if = ""
        else:
            akvorado_exporter = args.akvorado_exporter if args.akvorado_exporter else ("internet@" + args.host)
            akvorado_in_if = args.akvorado_in_if if args.akvorado_in_if else (
                (interface_label + ".0") if interface_label else None
            )
        if not boundary_only and not akvorado_in_if:
            print("Akvorado: задайте --akvorado-in-if (например ae5.0) или --akvorado-boundary-only (только InIfBoundary=external).", file=sys.stderr)
        else:
            akvorado_boundary = None if args.akvorado_no_boundary else "external"
            akvorado_table = (args.akvorado_table or "flows").strip()
            akv_bps_list, akv_ts, akv_err = fetch_akvorado_bps(
                args.akvorado_host, args.akvorado_user, args.akvorado_key,
                akvorado_exporter, akvorado_in_if,
                from_dt, to_dt,
                local_port=args.akvorado_port, in_if_boundary=akvorado_boundary,
                boundary_only=boundary_only,
                use_utc=not args.akvorado_tz_local, table_name=akvorado_table,
                interval_sec=args.akvorado_interval, debug=args.debug,
            )
            if akv_err:
                print("Akvorado: {}".format(akv_err), file=sys.stderr)
            elif not akv_bps_list:
                if boundary_only:
                    print("Akvorado: за период данных нет (InIfBoundary=external).", file=sys.stderr)
                else:
                    print("Akvorado: за период данных нет (ExporterName={}, InIfName={}).".format(
                        akvorado_exporter, akvorado_in_if), file=sys.stderr)
            else:
                p_akv = percentile_sorted(akv_bps_list, args.percentile)
                n_akv = len(akv_bps_list)
                print("")
                print("--- Akvorado (ClickHouse, flow, sample учтён) ---")
                if boundary_only:
                    print("InIfBoundary:     external")
                else:
                    print("ExporterName:     {}".format(akvorado_exporter))
                    print("InIfName:         {}".format(akvorado_in_if))
                print("Период:           {}".format(period_human))
                print("Точек (бакетов):  n = {}".format(n_akv))
                print("Перцентиль {:.0f}%: {}  ({})".format(args.percentile, human_bps(p_akv), p_akv))
                if args.check_gaps and akv_ts:
                    akv_gaps = find_gaps(akv_ts, time_from, time_till, args.akvorado_interval, tolerance_sec=max(1, args.akvorado_interval // 2))
                    print("")
                    print("Akvorado (ожидаемый шаг {} с):".format(args.akvorado_interval))
                    if not akv_gaps:
                        print("  данные за период непрерывны.")
                    else:
                        for g_start, g_end in akv_gaps:
                            print("  пропуск: {} — {}".format(format_ts_unix(g_start), format_ts_unix(g_end)))
                print("")
                print("--- Сравнение Zabbix vs Akvorado ---")
                print("Zabbix  {:.0f}%: {}  ({:.2f})".format(args.percentile, human_bps(p_val), p_val))
                print("Akvorado {:.0f}%: {}  ({:.2f})".format(args.percentile, human_bps(p_akv), p_akv))
                if p_val and p_val > 0:
                    diff_pct = (p_akv - p_val) / p_val * 100
                    print("Разница: {:.1f}% ({})".format(diff_pct, "Akvorado выше" if diff_pct > 0 else "Zabbix выше"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
