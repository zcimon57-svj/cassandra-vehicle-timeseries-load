#!/usr/bin/env python
"""Config-driven Cassandra vehicle time-series load generator.

The source is intentionally compatible with Python 2.7 and Python 3. Config
validation, dry-runs, and unit tests work without cassandra-driver.
"""

from __future__ import print_function

import argparse
import calendar
import copy
import glob
import hashlib
import heapq
import io
import json
import math
import multiprocessing
import os
import random
import re
import ssl
import signal
import sys
import threading
import time
import uuid
from collections import deque
from datetime import date, datetime, timedelta, tzinfo

try:
    import queue as queue_module
except ImportError:
    import Queue as queue_module

PY2 = sys.version_info[0] == 2
try:
    STRING_TYPES = (basestring,)  # noqa: F821 - defined by Python 2.
    INTEGER_TYPES = (int, long)  # noqa: F821 - defined by Python 2.
    TEXT_TYPE = unicode  # noqa: F821 - defined by Python 2.
    ITER_RANGE = xrange  # noqa: F821 - defined by Python 2.
except NameError:
    STRING_TYPES = (str,)
    INTEGER_TYPES = (int,)
    TEXT_TYPE = str
    ITER_RANGE = range


class _UtcTimezone(tzinfo):
    def utcoffset(self, _value):
        return timedelta(0)

    def dst(self, _value):
        return timedelta(0)

    def tzname(self, _value):
        return "UTC"


class _FixedOffsetTimezone(tzinfo):
    def __init__(self, minutes):
        self.offset = timedelta(minutes=minutes)

    def utcoffset(self, _value):
        return self.offset

    def dst(self, _value):
        return timedelta(0)

    def tzname(self, _value):
        total_minutes = int(self.offset.total_seconds() // 60)
        sign = "+" if total_minutes >= 0 else "-"
        total_minutes = abs(total_minutes)
        return "{}{:02d}:{:02d}".format(sign, total_minutes // 60, total_minutes % 60)


UTC_TZ = _UtcTimezone()
MONOTONIC_TIME = getattr(time, "monotonic", time.time)
VERSION = "2.0.0"


def _utc_now():
    return datetime.now(UTC_TZ)


def _datetime_to_epoch(value):
    utc_value = value.astimezone(UTC_TZ)
    return calendar.timegm(utc_value.utctimetuple()) + utc_value.microsecond / 1000000.0


def _epoch_to_datetime(value):
    return datetime.utcfromtimestamp(value).replace(tzinfo=UTC_TZ)


def _process_cpu_seconds():
    process_times = os.times()
    return process_times[0] + process_times[1]


def _fullmatch(pattern, value, flags=0):
    matcher = pattern if hasattr(pattern, "match") else re.compile(pattern, flags)
    match = matcher.match(value)
    if match is None or match.end() != len(value):
        return None
    return match


EXIT_OK = 0
EXIT_LOAD_FAILED = 1
EXIT_CONFIG_OR_ENV = 2
EXIT_INTERRUPTED = 130

IDENTIFIER_RE = re.compile(r"^[a-z][a-z0-9_]*$")
CREATE_KEYSPACE_RE = re.compile(r"^\s*CREATE\s+KEYSPACE\b", re.IGNORECASE)
CREATE_TABLE_RE = re.compile(r"^\s*CREATE\s+TABLE\b", re.IGNORECASE)

WRITE_CONSISTENCY_LEVELS = {
    "ANY",
    "ONE",
    "TWO",
    "THREE",
    "QUORUM",
    "ALL",
    "LOCAL_QUORUM",
    "EACH_QUORUM",
    "LOCAL_ONE",
}
READ_CONSISTENCY_LEVELS = {
    "ONE",
    "TWO",
    "THREE",
    "QUORUM",
    "ALL",
    "LOCAL_QUORUM",
    "LOCAL_ONE",
}

GENERATOR_KEYS = {
    "vehicle_id": set(),
    "timeline": set(),
    "time_window": set(),
    "event_time_seconds": set(),
    "event_time_millis": set(),
    "event_time_timestamp": set(),
    "event_date": set(),
    "sequence": set(),
    "stream_sequence": set(),
    "random_int": {"min", "max"},
    "random_float": {"min", "max", "decimals"},
    "choice": {"values"},
    "constant": {"value"},
    "linear_float": {"base", "step", "vehicle_step", "decimals"},
    "random_blob": {"size", "pool_size", "mode"},
    "random_text": {"size", "alphabet"},
    "boolean": {"true_probability"},
}


class ConfigError(ValueError):
    """Raised for an invalid or unsafe configuration."""


def _is_number(value):
    return isinstance(value, INTEGER_TYPES + (float,)) and not isinstance(value, bool)


def _require_mapping(value, path):
    if not isinstance(value, dict):
        raise ConfigError("{} must be a JSON object".format(path))
    return value


def _reject_unknown(mapping, allowed, path):
    unknown = sorted(set(mapping) - set(allowed))
    if unknown:
        raise ConfigError(
            "{} contains unknown field(s): {}".format(path, ", ".join(unknown))
        )


def _require_bool(value, path):
    if not isinstance(value, bool):
        raise ConfigError("{} must be true or false".format(path))
    return value


def _require_int(value, path, minimum=None):
    if not isinstance(value, INTEGER_TYPES) or isinstance(value, bool):
        raise ConfigError("{} must be an integer".format(path))
    if minimum is not None and value < minimum:
        raise ConfigError("{} must be >= {}".format(path, minimum))
    return value


def _require_number(value, path, minimum=None):
    if not _is_number(value) or math.isnan(float(value)) or math.isinf(float(value)):
        raise ConfigError("{} must be a finite number".format(path))
    result = float(value)
    if minimum is not None and result < minimum:
        raise ConfigError("{} must be >= {}".format(path, minimum))
    return result


def _require_string(value, path, allow_empty=False):
    if not isinstance(value, STRING_TYPES) or (not allow_empty and not value.strip()):
        suffix = "a string" if allow_empty else "a non-empty string"
        raise ConfigError("{} must be {}".format(path, suffix))
    return value


def _validate_identifier(value, path):
    result = _require_string(value, path)
    if not _fullmatch(IDENTIFIER_RE, result):
        raise ConfigError(
            "{}={!r} is not a simple Cassandra identifier; "
            "use lowercase letters, digits, and underscores, "
            "starting with a letter".format(path, result)
        )
    return result


def _parse_utc_timestamp(value, path="workload.reference_time_utc"):
    candidate = value.strip()
    match = _fullmatch(
        r"(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})"
        r"(?:\.(\d{1,6}))?([Zz]|[+-]\d{2}:?\d{2})",
        candidate,
    )
    if match is None:
        raise ConfigError(
            "{} must be an ISO-8601 timestamp with timezone, for example "
            "2026-09-01T00:00:00Z".format(path)
        )
    groups = match.groups()
    fraction = (groups[6] or "").ljust(6, "0")
    timezone_text = groups[7]
    if timezone_text.upper() == "Z":
        parsed_tz = UTC_TZ
    else:
        compact_offset = timezone_text.replace(":", "")
        sign = 1 if compact_offset[0] == "+" else -1
        offset_hours = int(compact_offset[1:3])
        offset_remainder = int(compact_offset[3:5])
        if (
            offset_remainder >= 60
            or offset_hours > 14
            or (offset_hours == 14 and offset_remainder != 0)
        ):
            raise ConfigError("{} timezone offset must be within +/-14:00".format(path))
        offset_minutes = sign * (offset_hours * 60 + offset_remainder)
        parsed_tz = _FixedOffsetTimezone(offset_minutes)
    try:
        parsed = datetime(
            int(groups[0]),
            int(groups[1]),
            int(groups[2]),
            int(groups[3]),
            int(groups[4]),
            int(groups[5]),
            int(fraction or "0"),
            tzinfo=parsed_tz,
        )
    except ValueError as exc:
        raise ConfigError("{} must be an ISO-8601 timestamp: {}".format(path, exc))
    return parsed.astimezone(UTC_TZ)


def _validate_create_cql(value, path, expected):
    cql = _require_string(value, path).strip()
    without_final_semicolon = cql[:-1] if cql.endswith(";") else cql
    if ";" in without_final_semicolon:
        raise ConfigError("{} must contain exactly one CQL statement".format(path))
    if not expected.match(cql):
        expected_name = (
            "CREATE KEYSPACE" if expected is CREATE_KEYSPACE_RE else "CREATE TABLE"
        )
        raise ConfigError("{} must start with {}".format(path, expected_name))
    return cql


def _extract_chs_column(cql):
    match = re.search(
        r"\bchs_column\s*['\"]?\s*:\s*['\"]?([a-z][a-z0-9_]*)",
        cql,
        re.IGNORECASE,
    )
    return match.group(1).lower() if match is not None else None


def load_json_config(path):
    try:
        with io.open(path, "r", encoding="utf-8") as config_file:
            content = config_file.read()
    except (IOError, OSError) as exc:
        raise ConfigError("cannot read config file {}: {}".format(path, exc))
    except UnicodeError as exc:
        raise ConfigError("config file must be valid UTF-8: {}: {}".format(path, exc))
    try:
        value = json.loads(content)
    except ValueError as exc:
        raise ConfigError(
            "invalid JSON in {} at line {}, column {}: {}".format(
                path,
                getattr(exc, "lineno", "?"),
                getattr(exc, "colno", "?"),
                getattr(exc, "msg", str(exc)),
            )
        )
    return _require_mapping(value, "config")


def validate_config(raw_config):
    """Validate and return a normalized deep copy of a JSON configuration."""

    config = copy.deepcopy(_require_mapping(dict(raw_config), "config"))
    _reject_unknown(
        config, {"connection", "schema", "workload", "verification", "report"}, "config"
    )

    connection = _require_mapping(config.setdefault("connection", {}), "connection")
    _reject_unknown(
        connection,
        {
            "contact_points",
            "port",
            "local_dc",
            "cassandra_home",
            "sessions",
            "username_env",
            "password_env",
            "connect_timeout_seconds",
            "request_timeout_seconds",
            "protocol_version",
            "ssl",
        },
        "connection",
    )
    contact_points = connection.setdefault("contact_points", ["127.0.0.1"])
    if not isinstance(contact_points, list) or not contact_points:
        raise ConfigError("connection.contact_points must be a non-empty JSON array")
    for index, point in enumerate(contact_points):
        _require_string(point, "connection.contact_points[{}]".format(index))
    _require_int(connection.setdefault("port", 9042), "connection.port", 1)
    if connection["port"] > 65535:
        raise ConfigError("connection.port must be <= 65535")
    _require_string(
        connection.setdefault("local_dc", ""), "connection.local_dc", allow_empty=True
    )
    _require_string(
        connection.setdefault("cassandra_home", ""),
        "connection.cassandra_home",
        allow_empty=True,
    )
    sessions = _require_int(
        connection.setdefault("sessions", 1), "connection.sessions", 1
    )
    if sessions > 16:
        raise ConfigError("connection.sessions must be <= 16")
    _require_string(
        connection.setdefault("username_env", ""),
        "connection.username_env",
        allow_empty=True,
    )
    _require_string(
        connection.setdefault("password_env", ""),
        "connection.password_env",
        allow_empty=True,
    )
    if bool(connection["username_env"]) != bool(connection["password_env"]):
        raise ConfigError(
            "connection.username_env and connection.password_env must be set together"
        )
    _require_number(
        connection.setdefault("connect_timeout_seconds", 10),
        "connection.connect_timeout_seconds",
        0.001,
    )
    _require_number(
        connection.setdefault("request_timeout_seconds", 30),
        "connection.request_timeout_seconds",
        0.001,
    )
    protocol_version = connection.setdefault("protocol_version", None)
    if protocol_version is not None:
        _require_int(protocol_version, "connection.protocol_version", 1)

    ssl_config = _require_mapping(connection.setdefault("ssl", {}), "connection.ssl")
    _reject_unknown(
        ssl_config,
        {"enabled", "ca_cert", "client_cert", "client_key", "check_hostname"},
        "connection.ssl",
    )
    _require_bool(ssl_config.setdefault("enabled", False), "connection.ssl.enabled")
    _require_string(
        ssl_config.setdefault("ca_cert", ""), "connection.ssl.ca_cert", True
    )
    _require_string(
        ssl_config.setdefault("client_cert", ""), "connection.ssl.client_cert", True
    )
    _require_string(
        ssl_config.setdefault("client_key", ""), "connection.ssl.client_key", True
    )
    _require_bool(
        ssl_config.setdefault("check_hostname", True), "connection.ssl.check_hostname"
    )
    if bool(ssl_config["client_cert"]) != bool(ssl_config["client_key"]):
        raise ConfigError(
            "connection.ssl.client_cert and client_key must be set together"
        )

    schema = _require_mapping(config.get("schema"), "schema")
    _reject_unknown(
        schema,
        {
            "keyspace",
            "table",
            "create_keyspace_if_missing",
            "create_table_if_missing",
            "keyspace_cql",
            "table_cql",
            "temperature_source",
            "temperature_column",
            "partition_key_columns",
            "require_chs_metadata",
            "truncate_before_load",
            "columns",
        },
        "schema",
    )
    _validate_identifier(schema.get("keyspace"), "schema.keyspace")
    _validate_identifier(schema.get("table"), "schema.table")
    _require_bool(
        schema.setdefault("require_chs_metadata", False), "schema.require_chs_metadata"
    )
    _require_bool(
        schema.setdefault("create_keyspace_if_missing", False),
        "schema.create_keyspace_if_missing",
    )
    _require_bool(
        schema.setdefault("create_table_if_missing", False),
        "schema.create_table_if_missing",
    )
    _require_bool(
        schema.setdefault("truncate_before_load", False), "schema.truncate_before_load"
    )
    temperature_source = _require_string(
        schema.setdefault("temperature_source", "write_timestamp"),
        "schema.temperature_source",
    )
    if temperature_source not in {"write_timestamp", "custom_ck"}:
        raise ConfigError(
            "schema.temperature_source must be write_timestamp or custom_ck"
        )
    temperature_column = _require_string(
        schema.setdefault("temperature_column", ""),
        "schema.temperature_column",
        allow_empty=True,
    )
    if temperature_source == "write_timestamp" and temperature_column:
        raise ConfigError("schema.temperature_column must be empty for write_timestamp")
    if temperature_source == "custom_ck":
        temperature_column = _validate_identifier(
            temperature_column, "schema.temperature_column"
        )
    partition_key_columns = schema.setdefault("partition_key_columns", ["vehicle_id"])
    if not isinstance(partition_key_columns, list) or not partition_key_columns:
        raise ConfigError("schema.partition_key_columns must be a non-empty JSON array")
    if len(set(partition_key_columns)) != len(partition_key_columns):
        raise ConfigError("schema.partition_key_columns contains duplicates")
    for index, name in enumerate(partition_key_columns):
        _validate_identifier(name, "schema.partition_key_columns[{}]".format(index))
    keyspace_cql = schema.setdefault("keyspace_cql", "")
    table_cql = schema.setdefault("table_cql", "")
    if schema["create_keyspace_if_missing"]:
        _validate_create_cql(keyspace_cql, "schema.keyspace_cql", CREATE_KEYSPACE_RE)
        if "{keyspace}" not in keyspace_cql:
            raise ConfigError(
                "schema.keyspace_cql must contain the {keyspace} placeholder"
            )
    else:
        _require_string(keyspace_cql, "schema.keyspace_cql", allow_empty=True)
    if schema["create_table_if_missing"]:
        _validate_create_cql(table_cql, "schema.table_cql", CREATE_TABLE_RE)
        if "{keyspace}" not in table_cql or "{table}" not in table_cql:
            raise ConfigError(
                "schema.table_cql must contain {keyspace} and {table} placeholders"
            )
        if temperature_source == "write_timestamp" and re.search(
            r"\bchs_column\b", table_cql, re.IGNORECASE
        ):
            raise ConfigError("write_timestamp table_cql must not specify chs_column")
        if temperature_source == "custom_ck":
            ddl_column = _extract_chs_column(table_cql)
            if ddl_column != temperature_column:
                raise ConfigError(
                    "custom_ck table_cql must set chs_column to {!r}".format(
                        temperature_column
                    )
                )
    else:
        _require_string(table_cql, "schema.table_cql", allow_empty=True)

    columns = schema.get("columns")
    if not isinstance(columns, list) or not columns:
        raise ConfigError("schema.columns must be a non-empty JSON array")
    seen_columns = set()
    seen_generators = set()
    column_generators = {}
    for index, raw_column in enumerate(columns):
        path = "schema.columns[{}]".format(index)
        column = _require_mapping(raw_column, path)
        generator = _require_string(
            column.get("generator"), "{}.generator".format(path)
        )
        if generator not in GENERATOR_KEYS:
            raise ConfigError(
                "{}.generator={!r} is unsupported; choose one of ".format(
                    path, generator
                )
                + ", ".join(sorted(GENERATOR_KEYS))
            )
        _reject_unknown(column, {"name", "generator"} | GENERATOR_KEYS[generator], path)
        name = _validate_identifier(column.get("name"), "{}.name".format(path))
        if name in seen_columns:
            raise ConfigError("duplicate schema column: {}".format(name))
        seen_columns.add(name)
        seen_generators.add(generator)
        column_generators[name] = generator
        _validate_column_generator(column, path)

    for generator in ("vehicle_id", "timeline"):
        if generator not in seen_generators:
            raise ConfigError(
                "schema.columns must contain a {!r} generator".format(generator)
            )
    if not seen_generators.intersection(
        {"event_time_seconds", "event_time_millis", "event_time_timestamp"}
    ):
        raise ConfigError("schema.columns must contain an event-time generator")
    for name in partition_key_columns:
        if name not in column_generators:
            raise ConfigError(
                "partition key column {!r} is not in schema.columns".format(name)
            )
    if temperature_source == "custom_ck":
        if temperature_column not in column_generators:
            raise ConfigError(
                "schema.temperature_column must also appear in schema.columns"
            )
        if column_generators[temperature_column] not in {
            "event_time_seconds",
            "event_time_millis",
            "event_time_timestamp",
        }:
            raise ConfigError(
                "schema.temperature_column must use an event-time generator"
            )

    workload = _require_mapping(config.get("workload"), "workload")
    _reject_unknown(
        workload,
        {
            "write_mode",
            "concurrency",
            "producer_threads",
            "batch_size",
            "total_rows",
            "duration_seconds",
            "rate_limit_rows_per_second",
            "vehicle_count",
            "vehicle_id_prefix",
            "vehicle_id_width",
            "event_time_mode",
            "timelines",
            "reference_time_utc",
            "time_windows",
            "timestamp_jitter_seconds",
            "ttl_seconds",
            "consistency_level",
            "max_retries",
            "retry_backoff_seconds",
            "max_errors",
            "progress_interval_seconds",
            "random_seed",
            "latency_sample_size",
            "processes",
            "run_id",
            "max_batch_bytes",
            "drain_timeout_seconds",
            "window_anchor",
            "window_sampling",
        },
        "workload",
    )
    write_mode = _require_string(
        workload.setdefault("write_mode", "async"), "workload.write_mode"
    )
    if write_mode not in {"async", "unlogged_batch", "sync"}:
        raise ConfigError("workload.write_mode must be async, unlogged_batch, or sync")
    if write_mode == "unlogged_batch":
        unsafe_partition_generators = [
            name
            for name in partition_key_columns
            if column_generators[name] not in {"vehicle_id", "constant"}
        ]
        if unsafe_partition_generators:
            raise ConfigError(
                "unlogged_batch requires partition key columns generated by "
                "vehicle_id or constant; unsafe columns: {}".format(
                    ", ".join(unsafe_partition_generators)
                )
            )
    concurrency = _require_int(
        workload.setdefault("concurrency", 256), "workload.concurrency", 1
    )
    producer_threads = _require_int(
        workload.setdefault("producer_threads", 1),
        "workload.producer_threads",
        1,
    )
    if write_mode != "sync" and producer_threads > concurrency:
        raise ConfigError(
            "workload.producer_threads must be <= concurrency for async modes"
        )
    batch_size = _require_int(
        workload.setdefault("batch_size", 8), "workload.batch_size", 2
    )
    if batch_size > 128:
        raise ConfigError("workload.batch_size must be <= 128")
    _require_int(workload.setdefault("processes", 1), "workload.processes", 1)
    if workload["processes"] > 64:
        raise ConfigError("workload.processes must be <= 64")
    _require_int(
        workload.setdefault("max_batch_bytes", 16384), "workload.max_batch_bytes", 256
    )
    _require_number(
        workload.setdefault("drain_timeout_seconds", 60),
        "workload.drain_timeout_seconds",
        1,
    )
    run_id = workload.setdefault("run_id", "auto")
    _require_string(run_id, "workload.run_id", allow_empty=True)
    if run_id and not _fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ConfigError("run_id must contain only letters, numbers, _ or -")
    if workload.setdefault("window_anchor", "fixed") not in {"fixed", "rolling"}:
        raise ConfigError("window_anchor must be fixed or rolling")
    if workload.setdefault("window_sampling", "sequential") not in {
        "sequential",
        "uniform",
    }:
        raise ConfigError("window_sampling must be sequential or uniform")
    _require_int(workload.setdefault("total_rows", 0), "workload.total_rows", 0)
    _require_number(
        workload.setdefault("duration_seconds", 0), "workload.duration_seconds", 0
    )
    _require_number(
        workload.setdefault("rate_limit_rows_per_second", 0),
        "workload.rate_limit_rows_per_second",
        0,
    )
    if workload["total_rows"] == 0 and workload["duration_seconds"] == 0:
        raise ConfigError(
            "set workload.total_rows, workload.duration_seconds, or both to > 0"
        )
    _require_int(workload.get("vehicle_count"), "workload.vehicle_count", 1)
    _require_string(
        workload.setdefault("vehicle_id_prefix", "vehicle-"),
        "workload.vehicle_id_prefix",
    )
    _require_int(
        workload.setdefault("vehicle_id_width", 8), "workload.vehicle_id_width", 1
    )
    event_time_mode = _require_string(
        workload.setdefault("event_time_mode", "natural_write_time"),
        "workload.event_time_mode",
    )
    if event_time_mode not in {"natural_write_time", "weighted_time_windows"}:
        raise ConfigError(
            "workload.event_time_mode must be natural_write_time or "
            "weighted_time_windows"
        )
    if (
        temperature_source == "write_timestamp"
        and event_time_mode == "weighted_time_windows"
    ):
        raise ConfigError(
            "weighted_time_windows can drive CHS temperature only when "
            "schema.temperature_source is custom_ck"
        )
    reference_time = workload.setdefault("reference_time_utc", None)
    if reference_time is not None:
        _parse_utc_timestamp(
            _require_string(reference_time, "workload.reference_time_utc"),
            "workload.reference_time_utc",
        )
    _require_number(
        workload.setdefault("timestamp_jitter_seconds", 0),
        "workload.timestamp_jitter_seconds",
        0,
    )
    ttl_seconds = workload.setdefault("ttl_seconds", None)
    if ttl_seconds is not None:
        _require_int(ttl_seconds, "workload.ttl_seconds", 1)
    consistency = _require_string(
        workload.setdefault("consistency_level", "LOCAL_ONE"),
        "workload.consistency_level",
    ).upper()
    workload["consistency_level"] = consistency
    if consistency not in WRITE_CONSISTENCY_LEVELS:
        raise ConfigError(
            "unsupported workload.consistency_level: {}".format(consistency)
        )
    _require_int(workload.setdefault("max_retries", 0), "workload.max_retries", 0)
    if workload["ttl_seconds"] is not None and workload["max_retries"]:
        raise ConfigError(
            "TTL loads require max_retries=0 to avoid extending expiry on retries"
        )
    _require_number(
        workload.setdefault("retry_backoff_seconds", 0.05),
        "workload.retry_backoff_seconds",
        0,
    )
    _require_int(workload.setdefault("max_errors", 0), "workload.max_errors", 0)
    _require_number(
        workload.setdefault("progress_interval_seconds", 5),
        "workload.progress_interval_seconds",
        0,
    )
    _require_int(workload.setdefault("random_seed", 20260903), "workload.random_seed")
    _require_int(
        workload.setdefault("latency_sample_size", 10000),
        "workload.latency_sample_size",
        1,
    )

    timelines = workload.get("timelines")
    if not isinstance(timelines, list) or not timelines:
        raise ConfigError("workload.timelines must be a non-empty JSON array")
    seen_timelines = set()
    for index, raw_timeline in enumerate(timelines):
        path = "workload.timelines[{}]".format(index)
        timeline = _require_mapping(raw_timeline, path)
        _reject_unknown(timeline, {"name", "interval_seconds"}, path)
        name = _require_string(timeline.get("name"), "{}.name".format(path))
        if name in seen_timelines:
            raise ConfigError("duplicate timeline name: {}".format(name))
        seen_timelines.add(name)
        _require_number(
            timeline.get("interval_seconds"), "{}.interval_seconds".format(path), 0.001
        )

    time_windows = workload.setdefault("time_windows", [])
    if not isinstance(time_windows, list):
        raise ConfigError("workload.time_windows must be a JSON array")
    if event_time_mode == "weighted_time_windows" and not time_windows:
        raise ConfigError(
            "workload.time_windows must be non-empty in weighted_time_windows mode"
        )
    seen_windows = set()
    for index, raw_window in enumerate(time_windows):
        path = "workload.time_windows[{}]".format(index)
        window = _require_mapping(raw_window, path)
        _reject_unknown(
            window,
            {"name", "weight", "start_offset_seconds", "end_offset_seconds"},
            path,
        )
        name = _require_string(window.get("name"), "{}.name".format(path))
        if name in seen_windows:
            raise ConfigError("duplicate time window name: {}".format(name))
        seen_windows.add(name)
        _require_int(window.get("weight"), "{}.weight".format(path), 1)
        start_offset = _require_number(
            window.get("start_offset_seconds"), "{}.start_offset_seconds".format(path)
        )
        end_offset = _require_number(
            window.get("end_offset_seconds"), "{}.end_offset_seconds".format(path)
        )
        if start_offset > end_offset:
            raise ConfigError(
                "{}.start_offset_seconds must be <= end_offset_seconds".format(path)
            )

    verification = _require_mapping(
        config.setdefault("verification", {}), "verification"
    )
    _reject_unknown(
        verification,
        {"enabled", "sample_size", "key_columns", "consistency_level"},
        "verification",
    )
    _require_bool(verification.setdefault("enabled", True), "verification.enabled")
    _require_int(
        verification.setdefault("sample_size", 20), "verification.sample_size", 1
    )
    key_columns = verification.get("key_columns")
    if not isinstance(key_columns, list) or not key_columns:
        raise ConfigError("verification.key_columns must be a non-empty JSON array")
    if len(set(key_columns)) != len(key_columns):
        raise ConfigError("verification.key_columns contains duplicates")
    for index, name in enumerate(key_columns):
        _validate_identifier(name, "verification.key_columns[{}]".format(index))
        if name not in seen_columns:
            raise ConfigError(
                "verification key column {!r} is not in schema.columns".format(name)
            )
    verification_consistency = _require_string(
        verification.setdefault("consistency_level", consistency),
        "verification.consistency_level",
    ).upper()
    verification["consistency_level"] = verification_consistency
    if verification_consistency not in READ_CONSISTENCY_LEVELS:
        raise ConfigError(
            "unsupported verification.consistency_level: {}".format(
                verification_consistency
            )
        )

    report = _require_mapping(config.setdefault("report", {}), "report")
    _reject_unknown(report, {"summary_json"}, "report")
    summary_json = report.setdefault("summary_json", "")
    _require_string(summary_json, "report.summary_json", allow_empty=True)
    return config


def _validate_column_generator(column, path):
    generator = column["generator"]
    if generator == "random_int":
        minimum = _require_int(column.get("min"), "{}.min".format(path))
        maximum = _require_int(column.get("max"), "{}.max".format(path))
        if minimum > maximum:
            raise ConfigError("{}.min must be <= {}.max".format(path, path))
    elif generator == "random_float":
        minimum = _require_number(column.get("min"), "{}.min".format(path))
        maximum = _require_number(column.get("max"), "{}.max".format(path))
        if minimum > maximum:
            raise ConfigError("{}.min must be <= {}.max".format(path, path))
        decimals = _require_int(
            column.get("decimals", 2), "{}.decimals".format(path), 0
        )
        if decimals > 12:
            raise ConfigError("{}.decimals must be <= 12".format(path))
    elif generator == "choice":
        values = column.get("values")
        if not isinstance(values, list) or not values:
            raise ConfigError("{}.values must be a non-empty JSON array".format(path))
        if any(value is None or isinstance(value, (list, dict)) for value in values):
            raise ConfigError(
                "{}.values must contain non-null JSON scalars".format(path)
            )
    elif generator == "constant":
        value = column.get("value")
        if value is None or isinstance(value, (list, dict)):
            raise ConfigError("{}.value must be a non-null JSON scalar".format(path))
    elif generator == "linear_float":
        _require_number(column.get("base"), "{}.base".format(path))
        _require_number(column.get("step"), "{}.step".format(path))
        _require_number(column.get("vehicle_step", 0), "{}.vehicle_step".format(path))
        decimals = _require_int(
            column.get("decimals", 2), "{}.decimals".format(path), 0
        )
        if decimals > 12:
            raise ConfigError("{}.decimals must be <= 12".format(path))
    elif generator in {"random_blob", "random_text"}:
        size = _require_int(column.get("size"), "{}.size".format(path), 1)
        if size > 16 * 1024 * 1024:
            raise ConfigError("{}.size must be <= 16777216".format(path))
        if generator == "random_blob":
            if column.setdefault("mode", "pooled") not in {"pooled", "unique"}:
                raise ConfigError("{}.mode must be pooled or unique".format(path))
            pool_size = _require_int(
                column.get("pool_size", 256), "{}.pool_size".format(path), 1
            )
            if size * pool_size > 64 * 1024 * 1024:
                raise ConfigError(
                    "{}.size * pool_size must be <= 67108864".format(path)
                )
        if generator == "random_text" and "alphabet" in column:
            _require_string(column["alphabet"], "{}.alphabet".format(path))
    elif generator == "boolean":
        probability = _require_number(
            column.get("true_probability", 0.5), "{}.true_probability".format(path), 0
        )
        if probability > 1:
            raise ConfigError("{}.true_probability must be <= 1".format(path))


def apply_overrides(config, args):
    result = copy.deepcopy(dict(config))
    workload = result["workload"]
    if getattr(args, "cassandra_home", None) is not None:
        result["connection"]["cassandra_home"] = args.cassandra_home
    if args.sessions is not None:
        result["connection"]["sessions"] = args.sessions
    if getattr(args, "processes", None) is not None:
        workload["processes"] = args.processes
    if getattr(args, "run_id", None) is not None:
        workload["run_id"] = args.run_id
    if args.write_mode is not None:
        workload["write_mode"] = args.write_mode
    if args.concurrency is not None:
        workload["concurrency"] = args.concurrency
    if args.producer_threads is not None:
        workload["producer_threads"] = args.producer_threads
    if args.batch_size is not None:
        workload["batch_size"] = args.batch_size
    if args.vehicle_id_prefix is not None:
        workload["vehicle_id_prefix"] = args.vehicle_id_prefix
    if args.rows is not None:
        workload["total_rows"] = args.rows
    if args.duration is not None:
        workload["duration_seconds"] = args.duration
    if args.rate is not None:
        workload["rate_limit_rows_per_second"] = args.rate
    if args.event_time_mode is not None:
        workload["event_time_mode"] = args.event_time_mode
    if args.summary_json is not None:
        result["report"]["summary_json"] = args.summary_json
    return validate_config(result)


def quote_identifier(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def qualified_table(config):
    schema = config["schema"]
    return "{}.{}".format(
        quote_identifier(schema["keyspace"]), quote_identifier(schema["table"])
    )


def build_insert_cql(config):
    columns = [
        quote_identifier(column["name"]) for column in config["schema"]["columns"]
    ]
    placeholders = ["?"] * len(columns)
    cql = "INSERT INTO {} ({}) VALUES ({})".format(
        qualified_table(config), ", ".join(columns), ", ".join(placeholders)
    )
    if config["workload"]["ttl_seconds"] is not None:
        cql += " USING TTL ?"
    return cql


def build_verify_cql(config):
    keys = config["verification"]["key_columns"]
    selected = ", ".join(
        quote_identifier(col["name"]) for col in config["schema"]["columns"]
    )
    predicates = " AND ".join("{} = ?".format(quote_identifier(name)) for name in keys)
    return "SELECT {} FROM {} WHERE {} LIMIT 1".format(
        selected, qualified_table(config), predicates
    )


def render_ddl(cql, config):
    schema = config["schema"]
    return cql.replace("{keyspace}", schema["keyspace"]).replace(
        "{table}", schema["table"]
    )


class RowContext:
    def __init__(
        self,
        index,
        vehicle_index,
        timeline_name,
        time_window_name,
        stream_sequence,
        event_epoch_seconds,
    ):
        self.index = index
        self.vehicle_index = vehicle_index
        self.timeline_name = timeline_name
        self.time_window_name = time_window_name
        self.stream_sequence = stream_sequence
        self.event_epoch_seconds = event_epoch_seconds


class VehicleRowGenerator:
    """Generate deterministic rows from a global sequence number."""

    def __init__(self, config, now_utc=None, vehicle_offset=0):
        self.config = config
        workload = config["workload"]
        if workload["reference_time_utc"] is not None:
            reference_time = _parse_utc_timestamp(
                workload["reference_time_utc"], "workload.reference_time_utc"
            )
        else:
            reference_time = (now_utc or _utc_now()).astimezone(UTC_TZ)
        self.reference_epoch_seconds = _datetime_to_epoch(reference_time)
        self.event_time_mode = workload["event_time_mode"]
        self.vehicle_count = workload["vehicle_count"]
        self.timelines = workload["timelines"]
        self.timeline_count = len(self.timelines)
        self.time_windows = workload["time_windows"]
        self.window_schedule = []
        if self.event_time_mode == "natural_write_time":
            self.window_schedule.append((None, 0))
        else:
            for window_index, window in enumerate(self.time_windows):
                for occurrence in ITER_RANGE(window["weight"]):
                    self.window_schedule.append((window_index, occurrence))
        self.window_slot_count = len(self.window_schedule)
        self.rows_per_vehicle = self.timeline_count * self.window_slot_count
        self.stream_count = self.vehicle_count * self.rows_per_vehicle
        self.seed = workload["random_seed"]
        self.vehicle_offset = vehicle_offset
        self.column_names = [c["name"] for c in config["schema"]["columns"]]
        self.local = threading.local()
        self.jitter_seconds = float(workload["timestamp_jitter_seconds"])
        self.blob_cache = {}
        self.blob_cache_lock = threading.Lock()

    def context(self, index, now_epoch_seconds=None):
        vehicle_index = (
            index // self.rows_per_vehicle
        ) % self.vehicle_count + self.vehicle_offset
        within_vehicle = index % self.rows_per_vehicle
        window_slot = within_vehicle // self.timeline_count
        timeline_index = within_vehicle % self.timeline_count
        logical_cycle = index // self.stream_count
        timeline = self.timelines[timeline_index]
        if self.event_time_mode == "natural_write_time":
            return RowContext(
                index=index,
                vehicle_index=vehicle_index,
                timeline_name=timeline["name"],
                time_window_name="natural",
                stream_sequence=logical_cycle,
                event_epoch_seconds=(
                    time.time() if now_epoch_seconds is None else now_epoch_seconds
                ),
            )
        window_index, window_occurrence = self.window_schedule[window_slot]
        window = self.time_windows[window_index]
        stream_sequence = logical_cycle * window["weight"] + window_occurrence
        anchor = self.reference_epoch_seconds
        if self.config["workload"]["window_anchor"] == "rolling":
            anchor = time.time() if now_epoch_seconds is None else now_epoch_seconds
        window_start = anchor + float(window["start_offset_seconds"])
        window_end = anchor + float(window["end_offset_seconds"])
        window_span = window_end - window_start
        progression = stream_sequence * float(timeline["interval_seconds"])
        if self.config["workload"]["window_sampling"] == "uniform":
            progression = (self._row_seed(index) / 4294967296.0) * window_span
        if window_span > 0:
            progression %= window_span
        else:
            progression = 0
        event_time = window_start + progression
        if self.jitter_seconds:
            jitter_rng = random.Random(self._row_seed(index) ^ 0x5DEECE66D)
            event_time += jitter_rng.uniform(-self.jitter_seconds, self.jitter_seconds)
            event_time = min(max(event_time, window_start), window_end)
        return RowContext(
            index=index,
            vehicle_index=vehicle_index,
            timeline_name=timeline["name"],
            time_window_name=window["name"],
            stream_sequence=stream_sequence,
            event_epoch_seconds=event_time,
        )

    def same_partition_index(self, logical_index, group_size):
        """Map each contiguous group to one vehicle partition without duplicates."""

        group_index = logical_index // group_size
        position_in_group = logical_index % group_size
        vehicle_index = group_index % self.vehicle_count
        vehicle_round = group_index // self.vehicle_count
        local_ordinal = vehicle_round * group_size + position_in_group
        logical_cycle = local_ordinal // self.rows_per_vehicle
        within_vehicle = local_ordinal % self.rows_per_vehicle
        return (
            logical_cycle * self.stream_count
            + vehicle_index * self.rows_per_vehicle
            + within_vehicle
        )

    def generate(self, index, now_epoch_seconds=None):
        context_now = now_epoch_seconds
        if self.event_time_mode == "natural_write_time" and context_now is None:
            context_now = 0.0
        context = self.context(index, now_epoch_seconds=context_now)
        rng = getattr(self.local, "rng", None)
        if rng is None:
            rng = self.local.rng = random.Random(0)
        rng.seed(self._row_seed(index))
        result = {}
        for column in self.config["schema"]["columns"]:
            result[column["name"]] = self._generate_value(column, context, rng)
        if self.event_time_mode == "natural_write_time":
            context.event_epoch_seconds = (
                time.time() if now_epoch_seconds is None else now_epoch_seconds
            )
            time_generators = {
                "event_time_seconds",
                "event_time_millis",
                "event_time_timestamp",
                "event_date",
            }
            for column in self.config["schema"]["columns"]:
                if column["generator"] in time_generators:
                    result[column["name"]] = self._generate_value(column, context, rng)
        return result

    def bind_values(self, row):
        values = [row[name] for name in self.column_names]
        ttl_seconds = self.config["workload"]["ttl_seconds"]
        if ttl_seconds is not None:
            values.append(ttl_seconds)
        return tuple(values)

    def _row_seed(self, index):
        # Keep the seed accepted by both runtimes. Each runtime is repeatable;
        # exact random metric values are not promised to match across Python 2/3.
        return ((self.seed & 0xFFFFFFFF) ^ ((index + 1) * 0x9E3779B9)) & 0xFFFFFFFF

    def _pooled_random_blob(self, column, context):
        size = column["size"]
        if column.get("mode", "pooled") == "unique":
            seed = "{}:{}:{}:{}".format(
                self.seed, column["name"], context.vehicle_index, context.index
            ).encode("ascii")
            blocks = [
                hashlib.sha256(seed + b":" + str(i).encode("ascii")).digest()
                for i in ITER_RANGE((size + 31) // 32)
            ]
            return bytearray(b"".join(blocks)[:size])
        pool_size = column.get("pool_size", 256)
        slot = context.index % pool_size
        cache_key = (column["name"], size, slot)
        cached = self.blob_cache.get(cache_key)
        if cached is not None:
            return cached
        seed = "{}:{}:{}".format(self.seed, column["name"], slot).encode("ascii")
        chunks = []
        generated = 0
        counter = 0
        while generated < size:
            digest = hashlib.sha256(seed + b":" + str(counter).encode("ascii")).digest()
            chunks.append(digest)
            generated += len(digest)
            counter += 1
        value = bytearray(b"".join(chunks)[:size])
        with self.blob_cache_lock:
            return self.blob_cache.setdefault(cache_key, value)

    def _generate_value(self, column, context, rng):
        generator = column["generator"]
        workload = self.config["workload"]
        if generator == "vehicle_id":
            return workload["vehicle_id_prefix"] + "{:0{}d}".format(
                context.vehicle_index, workload["vehicle_id_width"]
            )
        if generator == "timeline":
            return context.timeline_name
        if generator == "time_window":
            return context.time_window_name
        if generator == "event_time_seconds":
            return int(context.event_epoch_seconds)
        if generator == "event_time_millis":
            return int(context.event_epoch_seconds * 1000)
        if generator == "event_time_timestamp":
            return _epoch_to_datetime(context.event_epoch_seconds)
        if generator == "event_date":
            return _epoch_to_datetime(context.event_epoch_seconds).date()
        if generator == "sequence":
            return context.index
        if generator == "stream_sequence":
            return context.stream_sequence
        if generator == "random_int":
            return rng.randint(column["min"], column["max"])
        if generator == "random_float":
            return round(
                rng.uniform(float(column["min"]), float(column["max"])),
                column.get("decimals", 2),
            )
        if generator == "choice":
            return rng.choice(column["values"])
        if generator == "constant":
            return column["value"]
        if generator == "linear_float":
            value = (
                float(column["base"])
                + context.stream_sequence * float(column["step"])
                + context.vehicle_index * float(column.get("vehicle_step", 0))
            )
            return round(value, column.get("decimals", 2))
        if generator == "random_blob":
            return self._pooled_random_blob(column, context)
        if generator == "random_text":
            alphabet = column.get(
                "alphabet",
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
            )
            return "".join(rng.choice(alphabet) for _ in ITER_RANGE(column["size"]))
        if generator == "boolean":
            return rng.random() < float(column.get("true_probability", 0.5))
        raise AssertionError(
            "validated generator unexpectedly unsupported: {}".format(generator)
        )


class LoadController:
    def __init__(self, config):
        workload = config["workload"]
        verification = config["verification"]
        self.total_rows = workload["total_rows"]
        self.duration_seconds = float(workload["duration_seconds"])
        self.rate_limit = float(workload["rate_limit_rows_per_second"])
        self.max_errors = workload["max_errors"]
        self.max_latency_samples = workload["latency_sample_size"]
        self.verification_sample_size = (
            verification["sample_size"] if verification["enabled"] else 0
        )
        self.verification_keys = verification["key_columns"]
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.started_at = MONOTONIC_TIME()
        self.started_cpu_seconds = _process_cpu_seconds()
        self.next_index = 0
        self.in_flight = 0
        self.succeeded = 0
        self.failed = 0
        self.request_attempts = 0
        self.request_errors = 0
        self.requests_in_flight = 0
        self.completion_queued = 0
        self.latency_total = 0.0
        self.latency_max = 0.0
        self.queue_delay_total = 0.0
        self.retry_attempts = 0
        self.encoded_bytes = 0
        self.external_stop = None
        self.last_progress = None
        self.coordinator_requests = {}
        self.latency_seen = 0
        self.latency_samples = []
        self.latency_rng = random.Random(workload["random_seed"] ^ 0xC0FFEE)
        self.verify_rows = deque(maxlen=self.verification_sample_size)
        self.errors = []
        self.stop_reason = "running"

    @property
    def deadline(self):
        if self.duration_seconds <= 0:
            return None
        return self.started_at + self.duration_seconds

    def claim(self):
        claimed = self.claim_many(1)
        return claimed[0] if claimed else None

    def claim_many(self, max_count, stay_within_group=False):
        with self.lock:
            if self.external_stop is not None and self.external_stop.is_set():
                self._stop_locked("interrupted")
            if self.stop_event.is_set():
                return []
            if self.deadline is not None and MONOTONIC_TIME() >= self.deadline:
                self._stop_locked("duration")
                return []
            count = max_count
            if stay_within_group:
                count = min(count, max_count - (self.next_index % max_count))
            if self.total_rows > 0:
                remaining = self.total_rows - self.succeeded - self.in_flight
                if remaining <= 0:
                    return []
                count = min(count, remaining)
            first_index = self.next_index
            self.next_index += count
            self.in_flight += count
            return list(ITER_RANGE(first_index, first_index + count))

    def wait_for_rate(self, index):
        if self.rate_limit <= 0:
            return not self.stop_event.is_set()
        target = self.started_at + (index / self.rate_limit)
        while True:
            if self.stop_event.is_set():
                return False
            now = MONOTONIC_TIME()
            if self.deadline is not None and now >= self.deadline:
                with self.lock:
                    self._stop_locked("duration")
                return False
            delay = target - now
            if delay <= 0:
                return True
            if self.deadline is not None:
                delay = min(delay, max(0.0, self.deadline - now))
            self.stop_event.wait(min(delay, 0.25))

    def cancel_claim(self):
        self.cancel_claims(1)

    def cancel_claims(self, count):
        with self.lock:
            self.in_flight -= count

    def begin_request(self):
        with self.lock:
            self.requests_in_flight += 1

    def callback_received(self):
        with self.lock:
            self.requests_in_flight -= 1
            self.completion_queued += 1

    def record_request(
        self, latency_ms, failed, coordinator=None, from_queue=False, queue_delay_ms=0
    ):
        with self.lock:
            if from_queue:
                self.completion_queued -= 1
            else:
                self.requests_in_flight -= 1
            self.latency_total += latency_ms
            self.latency_max = max(self.latency_max, latency_ms)
            self.queue_delay_total += queue_delay_ms
            self.request_attempts += 1
            if failed:
                self.request_errors += 1
            if coordinator:
                self.coordinator_requests[coordinator] = (
                    self.coordinator_requests.get(coordinator, 0) + 1
                )
            self.latency_seen += 1
            if len(self.latency_samples) < self.max_latency_samples:
                self.latency_samples.append(latency_ms)
            else:
                replacement = self.latency_rng.randrange(self.latency_seen)
                if replacement < self.max_latency_samples:
                    self.latency_samples[replacement] = latency_ms

    def complete_success(self, row):
        self.complete_success_many([row])

    def complete_success_many(self, rows):
        with self.lock:
            self.in_flight -= len(rows)
            self.succeeded += len(rows)
            if self.verification_sample_size:
                for offset, row in enumerate(rows):
                    # Full-row reservoir; keep stable copies, not a tail-only sample.
                    if len(self.verify_rows) < self.verification_sample_size:
                        self.verify_rows.append(dict(row))
                    else:
                        slot = self.latency_rng.randrange(
                            self.succeeded - len(rows) + offset + 1
                        )
                        if slot < self.verification_sample_size:
                            self.verify_rows[slot] = dict(row)
            if self.total_rows > 0 and self.succeeded >= self.total_rows:
                self._stop_locked("rows")

    def complete_failure(self, error):
        self.complete_failure_many(error, 1)

    def complete_failure_many(self, error, row_count):
        with self.lock:
            self.in_flight -= row_count
            self.failed += row_count
            if len(self.errors) < 10:
                self.errors.append("{}: {}".format(type(error).__name__, error))
            if self.failed > self.max_errors:
                self._stop_locked("error_limit")

    def request_stop(self, reason):
        with self.lock:
            self._stop_locked(reason)

    def _stop_locked(self, reason):
        if self.stop_reason == "running" or reason in {
            "interrupted",
            "worker_exception",
        }:
            self.stop_reason = reason
        self.stop_event.set()

    def snapshot(self):
        with self.lock:
            elapsed = max(MONOTONIC_TIME() - self.started_at, 1e-9)
            client_cpu_percent = (
                (_process_cpu_seconds() - self.started_cpu_seconds) / elapsed * 100.0
            )
            latencies = sorted(self.latency_samples)
            return {
                "elapsed_seconds": elapsed,
                "claimed_rows": self.next_index,
                "in_flight": self.in_flight,
                "succeeded_rows": self.succeeded,
                "failed_rows": self.failed,
                "request_attempts": self.request_attempts,
                "request_errors": self.request_errors,
                "requests_in_flight": self.requests_in_flight,
                "completion_queued": self.completion_queued,
                "retry_attempts": self.retry_attempts,
                "encoded_bytes": self.encoded_bytes,
                "queue_delay_ms_mean": self.queue_delay_total
                / max(1, self.request_attempts),
                "rows_per_second": self.succeeded / elapsed,
                "requests_per_second": self.request_attempts / elapsed,
                "client_cpu_percent": client_cpu_percent,
                "coordinator_requests": dict(self.coordinator_requests),
                "latency_ms": {
                    "sample_count": len(latencies),
                    "p50": _percentile(latencies, 0.50),
                    "p95": _percentile(latencies, 0.95),
                    "p99": _percentile(latencies, 0.99),
                    "max": self.latency_max,
                    "mean": self.latency_total / max(1, self.request_attempts),
                },
                "stop_reason": self.stop_reason,
                "errors": list(self.errors),
            }


def _percentile(sorted_values, percentile):
    if not sorted_values:
        return 0.0
    index = int(
        min(len(sorted_values) - 1, math.ceil(len(sorted_values) * percentile) - 1)
    )
    return round(float(sorted_values[index]), 3)


def _sync_worker(
    session,
    prepared,
    config,
    generator,
    controller,
):
    workload = config["workload"]
    max_attempts = workload["max_retries"] + 1
    timeout = float(config["connection"]["request_timeout_seconds"])
    while True:
        index = controller.claim()
        if index is None:
            return
        if not controller.wait_for_rate(index):
            controller.cancel_claim()
            return
        row = generator.generate(index)
        values = generator.bind_values(row)
        final_error = None
        for attempt in ITER_RANGE(max_attempts):
            started_at = MONOTONIC_TIME()
            controller.begin_request()
            try:
                session.execute(prepared, values, timeout=timeout)
                controller.record_request(
                    (MONOTONIC_TIME() - started_at) * 1000, failed=False
                )
                controller.complete_success(row)
                final_error = None
                break
            except Exception as exc:  # noqa: BLE001 - driver exceptions vary by release.
                final_error = exc
                controller.record_request(
                    (MONOTONIC_TIME() - started_at) * 1000, failed=True
                )
                if attempt + 1 < max_attempts:
                    backoff = float(workload["retry_backoff_seconds"]) * (2**attempt)
                    controller.stop_event.wait(backoff)
                    if controller.stop_event.is_set():
                        break
        if final_error is not None:
            controller.complete_failure(final_error)


class _AsyncCompletion:
    def __init__(self, started_at, completed_queue, controller):
        self.started_at = started_at
        self.completed_at = None
        self.error = None
        self.request = None
        self.future = None
        self.coordinator = None
        self.completed_queue = completed_queue
        self.controller = controller

    def capture_coordinator(self):
        host = getattr(self.future, "coordinator_host", None)
        if host is None:
            return
        endpoint = getattr(host, "endpoint", None)
        address = getattr(endpoint, "address", None) or getattr(host, "address", None)
        port = getattr(endpoint, "port", None)
        if address is not None and port is not None:
            self.coordinator = "{}:{}".format(address, port)
        elif address is not None:
            self.coordinator = str(address)
        else:
            self.coordinator = str(host)

    def on_success(self, _result):
        self.capture_coordinator()
        self.completed_at = MONOTONIC_TIME()
        self.controller.callback_received()
        self.completed_queue.put(self)

    def on_error(self, error):
        self.error = error
        self.capture_coordinator()
        self.completed_at = MONOTONIC_TIME()
        self.controller.callback_received()
        self.completed_queue.put(self)


def _make_async_statement(prepared, config, value_rows):
    if config["workload"]["write_mode"] != "unlogged_batch":
        if hasattr(prepared, "bind"):
            statement = prepared.bind(value_rows[0])
            statement._load_bytes = sum(
                4 + (len(v) if v is not None else 0) for v in statement.values
            )
            return statement, None
        return prepared, value_rows[0]
    from cassandra.query import BatchStatement, BatchType

    statement = BatchStatement(
        batch_type=BatchType.UNLOGGED,
        consistency_level=_consistency_value(config["workload"]["consistency_level"]),
    )
    routing_key = None
    # Protocol header/options allowance plus each prepared-id/value tuple.
    # The statement kind, short query-id length and short value count need
    # five bytes in addition to the query id itself.
    size = 64
    for values in value_rows:
        bound = prepared.bind(values)
        key = bound.routing_key
        if key is None or (routing_key is not None and key != routing_key):
            raise ConfigError("batch must contain one actual encoded partition key")
        routing_key = key
        size += (
            5
            + len(prepared.query_id)
            + sum(4 + (len(v) if v is not None else 0) for v in bound.values)
        )
        if size > config["workload"]["max_batch_bytes"]:
            raise ConfigError(
                "batch exceeds max_batch_bytes; lower batch_size or payload size"
            )
        statement.add(bound)
    statement._load_bytes = size
    return statement, None


def _submit_async_request(
    session,
    prepared,
    config,
    rows,
    value_rows,
    attempt,
    controller,
    completed_queue,
):
    if isinstance(session, (list, tuple)):
        cursor = getattr(controller, "session_cursor", 0)
        controller.session_cursor = cursor + 1
        session = session[cursor % len(session)]
    completion = _AsyncCompletion(MONOTONIC_TIME(), completed_queue, controller)
    request = {
        "rows": rows,
        "value_rows": value_rows,
        "attempt": attempt,
    }
    completion.request = request
    controller.begin_request()
    try:
        statement, parameters = _make_async_statement(prepared, config, value_rows)
        controller.encoded_bytes += getattr(statement, "_load_bytes", 0)
        timeout = float(config["connection"]["request_timeout_seconds"])
        if parameters is None:
            future = session.execute_async(statement, timeout=timeout)
        else:
            future = session.execute_async(statement, parameters, timeout=timeout)
        completion.future = future
        future.add_callbacks(completion.on_success, completion.on_error)
    except Exception as exc:  # noqa: BLE001 - driver exceptions vary by release.
        completion.on_error(exc)
    return request


def _async_worker(
    session,
    prepared,
    config,
    generator,
    controller,
    request_window,
):
    workload = config["workload"]
    write_mode = workload["write_mode"]
    rows_per_request = workload["batch_size"] if write_mode == "unlogged_batch" else 1
    completed_queue = queue_module.Queue()
    outstanding = 0
    retries = []
    retry_order = 0
    stopping_at = None
    completion = None
    while True:
        now = MONOTONIC_TIME()
        if controller.external_stop is not None and controller.external_stop.is_set():
            controller.request_stop("interrupted")
        if controller.deadline is not None and now >= controller.deadline:
            controller.request_stop("duration")
        stopping = controller.stop_event.is_set()
        if stopping:
            if stopping_at is None:
                stopping_at = now
            while retries:
                _, _, request = heapq.heappop(retries)
                controller.complete_failure_many(
                    RuntimeError("stopped before retry"), len(request["rows"])
                )
            if not outstanding:
                return
            if now - stopping_at > workload["drain_timeout_seconds"]:
                raise RuntimeError(
                    "drain deadline exceeded; outstanding writes have unknown outcome"
                )

        # Harvest before generating or rate-limiting any more work.
        if completion is None:
            try:
                completion = completed_queue.get_nowait()
            except queue_module.Empty:
                pass
        if completion is not None:
            outstanding -= 1
            request = completion.request
            controller.record_request(
                (completion.completed_at - completion.started_at) * 1000,
                failed=completion.error is not None,
                coordinator=completion.coordinator,
                from_queue=True,
                queue_delay_ms=(now - completion.completed_at) * 1000,
            )
            error = completion.error
            completion.future = completion.request = completion.controller = None
            completion = None
            if error is None:
                controller.complete_success_many(request["rows"])
            elif (
                not stopping
                and request["attempt"] <= workload["max_retries"]
                and _retryable(error)
            ):
                retry_order += 1
                delay = workload["retry_backoff_seconds"] * (
                    2 ** (request["attempt"] - 1)
                )
                heapq.heappush(retries, (now + delay, retry_order, request))
            else:
                controller.complete_failure_many(error, len(request["rows"]))
            continue

        if (
            not stopping
            and retries
            and retries[0][0] <= now
            and outstanding < request_window
        ):
            _, _, request = heapq.heappop(retries)
            controller.retry_attempts += 1
            _submit_async_request(
                session,
                prepared,
                config,
                request["rows"],
                request["value_rows"],
                request["attempt"] + 1,
                controller,
                completed_queue,
            )
            outstanding += 1
            continue

        rate_delay = 0.0
        if controller.rate_limit:
            last_index = controller.next_index + rows_per_request - 1
            if controller.total_rows:
                last_index = min(
                    last_index,
                    controller.next_index
                    + max(
                        0,
                        controller.total_rows
                        - controller.succeeded
                        - controller.in_flight,
                    )
                    - 1,
                )
            rate_delay = (
                controller.started_at + last_index / controller.rate_limit - now
            )
        if (
            not stopping
            and outstanding + len(retries) < request_window
            and rate_delay <= 0
        ):
            indexes = controller.claim_many(
                rows_per_request, stay_within_group=(write_mode == "unlogged_batch")
            )
            if indexes:
                if write_mode == "unlogged_batch":
                    indexes = [
                        generator.same_partition_index(i, workload["batch_size"])
                        for i in indexes
                    ]
                rows = [generator.generate(i) for i in indexes]
                values = [generator.bind_values(row) for row in rows]
                _submit_async_request(
                    session,
                    prepared,
                    config,
                    rows,
                    values,
                    1,
                    controller,
                    completed_queue,
                )
                outstanding += 1
                continue
        timeout = 0.02
        if rate_delay > 0:
            timeout = min(timeout, rate_delay)
        if retries:
            timeout = min(timeout, max(0.0001, retries[0][0] - now))
        try:
            completion = completed_queue.get(timeout=timeout)
        except queue_module.Empty:
            pass


def _retryable(error):
    # Driver-independent names keep the stdlib-only self-test usable.
    return type(error).__name__ in {
        "OperationTimedOut",
        "WriteTimeout",
        "Unavailable",
        "Overloaded",
        "NoHostAvailable",
        "SyntheticRetryable",
    }


def _worker_guard(
    worker_function,
    worker_args,
    controller,
    worker_state,
    state_lock,
    all_workers_done,
):
    try:
        worker_function(*worker_args)
    except Exception as exc:  # noqa: BLE001 - propagate arbitrary worker failures.
        with state_lock:
            worker_state["errors"].append(exc)
        controller.request_stop("worker_exception")
    finally:
        with state_lock:
            worker_state["remaining"] -= 1
            if worker_state["remaining"] == 0:
                all_workers_done.set()


def _run_worker_specs(
    worker_specs, controller, config, show_progress, progress_callback=None
):
    progress_interval = float(config["workload"]["progress_interval_seconds"])
    worker_state = {"remaining": len(worker_specs), "errors": []}
    state_lock = threading.Lock()
    all_workers_done = threading.Event()
    threads = []
    try:
        for worker_index, worker_spec in enumerate(worker_specs):
            worker_function, worker_args = worker_spec
            thread = threading.Thread(
                name="cass-load-{}".format(worker_index),
                target=_worker_guard,
                args=(
                    worker_function,
                    worker_args,
                    controller,
                    worker_state,
                    state_lock,
                    all_workers_done,
                ),
            )
            thread.start()
            threads.append(thread)
        next_progress = MONOTONIC_TIME() + progress_interval
        while not all_workers_done.is_set():
            if (
                controller.external_stop is not None
                and controller.external_stop.is_set()
            ):
                controller.request_stop("interrupted")
            timeout = 0.5
            if progress_interval > 0:
                timeout = max(0.05, min(0.5, next_progress - MONOTONIC_TIME()))
            all_workers_done.wait(timeout)
            if (
                (show_progress or progress_callback)
                and progress_interval > 0
                and MONOTONIC_TIME() >= next_progress
            ):
                snapshot = controller.snapshot()
                if progress_callback:
                    snapshot["_latency_samples"] = list(controller.latency_samples)
                    progress_callback(snapshot)
                if show_progress:
                    print_progress(snapshot, config)
                next_progress = MONOTONIC_TIME() + progress_interval
    except KeyboardInterrupt:
        print(
            "\n>>> Interrupt received; stopping new work and "
            "draining in-flight requests",
            file=sys.stderr,
        )
        sys.stderr.flush()
        controller.request_stop("interrupted")
        raise
    finally:
        for thread in threads:
            thread.join()
    if worker_state["errors"]:
        raise worker_state["errors"][0]


def run_load(
    session,
    prepared,
    config,
    generator=None,
    show_progress=True,
    progress_callback=None,
    start_at=None,
    external_stop=None,
):
    generator = generator or VehicleRowGenerator(config)
    controller = LoadController(config)
    if start_at is not None:
        controller.started_at = start_at
    controller.external_stop = external_stop
    workload = config["workload"]
    concurrency = workload["concurrency"]
    sessions = list(session) if isinstance(session, (list, tuple)) else [session]
    if workload["write_mode"] == "sync":
        worker_specs = [
            (
                _sync_worker,
                (
                    sessions[worker_index % len(sessions)],
                    prepared,
                    config,
                    generator,
                    controller,
                ),
            )
            for worker_index in ITER_RANGE(concurrency)
        ]
    else:
        producer_count = min(workload["producer_threads"], concurrency)
        base_window = concurrency // producer_count
        extra_windows = concurrency % producer_count
        worker_specs = []
        for producer_index in ITER_RANGE(producer_count):
            request_window = base_window + (1 if producer_index < extra_windows else 0)
            worker_specs.append(
                (
                    _async_worker,
                    (
                        sessions,
                        prepared,
                        config,
                        generator,
                        controller,
                        request_window,
                    ),
                )
            )
    _run_worker_specs(
        worker_specs, controller, config, show_progress, progress_callback
    )
    summary = controller.snapshot()
    summary["_latency_samples"] = list(controller.latency_samples)
    summary["write_mode"] = workload["write_mode"]
    summary["configured_concurrency"] = concurrency
    summary["sessions"] = len(sessions)
    summary["producer_threads"] = (
        concurrency if workload["write_mode"] == "sync" else producer_count
    )
    summary["batch_size"] = (
        workload["batch_size"] if workload["write_mode"] == "unlogged_batch" else 1
    )
    return summary, list(controller.verify_rows)


def print_progress(summary, config):
    total = config["workload"]["total_rows"]
    completed = summary["succeeded_rows"]
    progress = "/{} ({:.1f}%)".format(total, completed * 100.0 / total) if total else ""
    latency = summary["latency_ms"]
    workload = config["workload"]
    effective_batch_size = (
        workload["batch_size"] if workload["write_mode"] == "unlogged_batch" else 1
    )
    coordinator_text = ",".join(
        "{}={}".format(host, count)
        for host, count in sorted(summary["coordinator_requests"].items())
    )
    print(
        ">>> progress utc={} elapsed={:.1f}s mode={} batch={} rows={}{} failed={} "
        "inflight_req={} inflight_rows={} row_rate={:.1f}/s req_rate={:.1f}/s "
        "p95={:.3f}ms client_cpu={:.1f}% coordinators={}".format(
            _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            summary["elapsed_seconds"],
            workload["write_mode"],
            effective_batch_size,
            completed,
            progress,
            summary["failed_rows"],
            summary["requests_in_flight"],
            summary["in_flight"],
            summary["rows_per_second"],
            summary["requests_per_second"],
            latency["p95"],
            summary["client_cpu_percent"],
            coordinator_text or "unavailable",
        ),
    )
    sys.stdout.flush()


class _SelfTestFuture:
    def __init__(self, error=None):
        self.error = error

    def add_callbacks(self, callback, errback):
        if self.error is None:
            callback(None)
        else:
            errback(self.error)


class SyntheticRetryable(RuntimeError):
    pass


class _SelfTestSession:
    def __init__(self, fail_first_async=False):
        self.lock = threading.Lock()
        self.rows = []
        self.async_calls = 0
        self.fail_first_async = fail_first_async

    def execute(self, _prepared, values, timeout=None):
        del timeout
        with self.lock:
            self.rows.append(values)

    def execute_async(self, _prepared, values=None, timeout=None):
        del timeout
        with self.lock:
            self.async_calls += 1
            if self.fail_first_async and self.async_calls == 1:
                return _SelfTestFuture(SyntheticRetryable("synthetic async failure"))
            self.rows.append(values)
        return _SelfTestFuture()


def run_self_test(config):
    test_config = validate_config(
        {
            "connection": {},
            "schema": {
                "keyspace": "self_test",
                "table": "vehicle",
                "columns": [
                    {"name": "vehicle_id", "generator": "vehicle_id"},
                    {"name": "event_time_s", "generator": "event_time_seconds"},
                    {"name": "timeline", "generator": "timeline"},
                    {"name": "sample_seq", "generator": "sequence"},
                    {"name": "time_window", "generator": "time_window"},
                ],
            },
            "workload": {
                "vehicle_count": 2,
                "total_rows": 257,
                "max_retries": 1,
                "timelines": [
                    {"name": name, "interval_seconds": 1}
                    for name in ("gps", "powertrain", "battery")
                ],
                "time_windows": [
                    {
                        "name": "cold",
                        "weight": 4,
                        "start_offset_seconds": -604800,
                        "end_offset_seconds": -86400,
                    },
                    {
                        "name": "hot",
                        "weight": 1,
                        "start_offset_seconds": -300,
                        "end_offset_seconds": 0,
                    },
                ],
            },
            "verification": {
                "key_columns": ["vehicle_id", "event_time_s", "timeline", "sample_seq"]
            },
        }
    )
    test_config["schema"]["create_keyspace_if_missing"] = False
    test_config["schema"]["create_table_if_missing"] = False
    test_config["schema"]["temperature_source"] = "custom_ck"
    test_config["schema"]["temperature_column"] = "event_time_s"
    test_config["verification"]["enabled"] = False
    test_config["workload"].update(
        {
            "write_mode": "async",
            "concurrency": 8,
            "producer_threads": 4,
            "batch_size": 8,
            "total_rows": 257,
            "duration_seconds": 0,
            "rate_limit_rows_per_second": 0,
            "vehicle_count": 2,
            "event_time_mode": "weighted_time_windows",
            "reference_time_utc": "2026-09-01T00:00:00Z",
            "progress_interval_seconds": 0,
        }
    )
    test_config = validate_config(test_config)
    generator = VehicleRowGenerator(test_config)
    preview = [generator.generate(index) for index in ITER_RANGE(15)]
    window_counts = {}
    timeline_counts = {}
    for row in preview:
        window_counts[row["time_window"]] = window_counts.get(row["time_window"], 0) + 1
        timeline_counts[row["timeline"]] = timeline_counts.get(row["timeline"], 0) + 1
    if window_counts != {"cold": 12, "hot": 3}:
        raise RuntimeError(
            "self-test window distribution mismatch: {}".format(window_counts)
        )
    if timeline_counts != {"gps": 5, "powertrain": 5, "battery": 5}:
        raise RuntimeError(
            "self-test timeline distribution mismatch: {}".format(timeline_counts)
        )
    mapped_indexes = []
    for group_start in ITER_RANGE(0, 32, 8):
        group_indexes = [
            generator.same_partition_index(index, 8)
            for index in ITER_RANGE(group_start, group_start + 8)
        ]
        mapped_indexes.extend(group_indexes)
        vehicle_ids = {
            generator.generate(index)["vehicle_id"] for index in group_indexes
        }
        if len(vehicle_ids) != 1:
            raise RuntimeError("self-test batch crossed vehicle partitions")
    if len(set(mapped_indexes)) != len(mapped_indexes):
        raise RuntimeError("self-test batch index mapping produced duplicates")
    unsafe_batch_config = copy.deepcopy(test_config)
    unsafe_batch_config["workload"]["write_mode"] = "unlogged_batch"
    unsafe_batch_config["schema"]["partition_key_columns"] = ["event_time_s"]
    try:
        validate_config(unsafe_batch_config)
    except ConfigError:
        pass
    else:
        raise RuntimeError("self-test accepted an unsafe cross-partition batch schema")

    sessions = [
        _SelfTestSession(fail_first_async=True),
        _SelfTestSession(),
    ]
    summary, _ = run_load(
        sessions,
        prepared=object(),
        config=test_config,
        generator=generator,
        show_progress=False,
    )
    if summary["succeeded_rows"] != 257 or summary["failed_rows"] != 0:
        raise RuntimeError("self-test concurrent load mismatch: {}".format(summary))
    if summary["write_mode"] != "async" or summary["request_attempts"] != 258:
        raise RuntimeError("self-test async transport mismatch: {}".format(summary))
    if summary["sessions"] != 2:
        raise RuntimeError("self-test session fanout mismatch: {}".format(summary))
    if summary["request_errors"] != 1:
        raise RuntimeError("self-test async retry mismatch: {}".format(summary))
    if summary["requests_in_flight"] != 0 or summary["in_flight"] != 0:
        raise RuntimeError("self-test left in-flight work: {}".format(summary))
    session_row_counts = [len(session.rows) for session in sessions]
    if sum(session_row_counts) != 257 or not all(session_row_counts):
        raise RuntimeError("self-test fake session row count mismatch")

    natural_config = copy.deepcopy(test_config)
    natural_config["schema"]["temperature_source"] = "write_timestamp"
    natural_config["schema"]["temperature_column"] = ""
    natural_config["workload"]["event_time_mode"] = "natural_write_time"
    natural_config["workload"]["time_windows"] = []
    natural_config = validate_config(natural_config)
    natural_row = VehicleRowGenerator(natural_config).generate(
        7, now_epoch_seconds=1788220999.875
    )
    if natural_row["event_time_s"] != 1788220999:
        raise RuntimeError("self-test natural event time mismatch")
    if natural_row["time_window"] != "natural":
        raise RuntimeError("self-test natural window label mismatch")

    return {
        "python": sys.version.split()[0],
        "stdlib_only": True,
        "concurrent_rows": summary["succeeded_rows"],
        "async_requests": summary["request_attempts"],
        "session_row_counts": session_row_counts,
        "window_counts": window_counts,
        "timeline_counts": timeline_counts,
        "same_partition_batch_groups": 4,
        "unsafe_batch_schema_rejected": True,
        "natural_event_time": natural_row["event_time_s"],
    }


def _consistency_value(name):
    from cassandra import ConsistencyLevel

    return getattr(ConsistencyLevel, name)


def prepare_insert(session, config):
    statement = session.prepare(build_insert_cql(config))
    statement.consistency_level = _consistency_value(
        config["workload"]["consistency_level"]
    )
    if hasattr(statement, "is_idempotent"):
        statement.is_idempotent = config["workload"]["ttl_seconds"] is None
    return statement


def prepare_verification(session, config):
    if not config["verification"]["enabled"]:
        return None
    statement = session.prepare(build_verify_cql(config))
    statement.consistency_level = _consistency_value(
        config["verification"]["consistency_level"]
    )
    return statement


def run_verification(
    session,
    config,
    sample_rows,
    statement=None,
):
    verification = config["verification"]
    if not verification["enabled"]:
        return {"enabled": False, "checked": 0, "found": 0, "missing": 0}
    statement = statement or prepare_verification(session, config)
    keys = verification["key_columns"]
    found = 0
    missing_keys = []
    mismatches = []
    timeout = float(config["connection"]["request_timeout_seconds"])
    for row in sample_rows:
        values = tuple(row[name] for name in keys)
        result = next(iter(session.execute(statement, values, timeout=timeout)), None)
        if result is None:
            missing_keys.append({name: _json_value(row[name]) for name in keys})
        else:
            found += 1
            actual = result if isinstance(result, dict) else result._asdict()
            for column in config["schema"]["columns"]:
                name = column["name"]
                if not _values_equal(row.get(name), actual.get(name)):
                    mismatches.append(
                        {"column": name, "keys": {k: _json_value(row[k]) for k in keys}}
                    )
    return {
        "enabled": True,
        "checked": len(sample_rows),
        "found": found,
        "missing": len(missing_keys),
        "missing_keys": missing_keys[:10],
        "mismatched_values": len(mismatches),
        "mismatch_samples": mismatches[:10],
    }


def _values_equal(expected, actual):
    if isinstance(expected, bytearray):
        return actual is not None and bytearray(actual) == expected
    if isinstance(expected, datetime) and isinstance(actual, datetime):
        # CQL timestamp persists milliseconds, not Python's microseconds.
        # Drivers return UTC-naive datetimes; normalize aware inputs to UTC.
        if expected.utcoffset() is not None:
            expected = expected - expected.utcoffset()
        if actual.utcoffset() is not None:
            actual = actual - actual.utcoffset()
        expected = expected.replace(
            tzinfo=None, microsecond=(expected.microsecond // 1000) * 1000
        )
        return expected == actual.replace(tzinfo=None)
    if isinstance(expected, date) and not isinstance(expected, datetime):
        # Python 2 date.__eq__ does not delegate to cassandra.util.Date.
        to_date = getattr(actual, "date", None)
        if callable(to_date):
            actual = to_date()
        return expected == actual
    if isinstance(expected, float) and isinstance(actual, (int, float)):
        return abs(expected - actual) <= max(1e-6, abs(expected) * 1e-6)
    return expected == actual


def _cqlsh_library_dirs(cassandra_home):
    locations = []
    requested = cassandra_home or os.environ.get("CASSANDRA_HOME", "")
    if requested:
        requested = os.path.abspath(os.path.expanduser(requested))
        if os.path.isfile(requested) and os.path.basename(requested) == "cqlsh.py":
            requested = os.path.dirname(os.path.dirname(requested))
        if os.path.basename(requested) == "lib":
            locations.append(requested)
        else:
            locations.append(os.path.join(requested, "lib"))
    if sys.platform.startswith("linux"):
        locations.append("/usr/share/cassandra/lib")
    result = []
    seen = set()
    for location in locations:
        normalized = os.path.realpath(location)
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def _bundled_cqlsh_paths(cassandra_home):
    """Return driver and companion zip paths using Cassandra 3.11 cqlsh layout."""

    if os.environ.get("CQLSH_NO_BUNDLED", "") and not cassandra_home:
        return [], []
    searched = _cqlsh_library_dirs(cassandra_home)
    for lib_dir in searched:
        driver_zips = glob.glob(
            os.path.join(lib_dir, "cassandra-driver-internal-only-*.zip")
        )
        if not driver_zips:
            continue
        driver_zip = max(
            driver_zips,
            key=lambda p: tuple(
                int(n) for n in re.findall(r"\d+", os.path.basename(p))
            ),
        )
        filename = os.path.splitext(os.path.basename(driver_zip))[0]
        version = filename[len("cassandra-driver-internal-only-") :]
        paths = [os.path.join(driver_zip, "cassandra-driver-" + version)]
        for prefix in ("futures-", "six-"):
            companion_zips = glob.glob(os.path.join(lib_dir, prefix + "*.zip"))
            if companion_zips:
                paths.append(max(companion_zips))
        return paths, searched
    return [], searched


def _load_cassandra_driver(cassandra_home):
    paths, searched = _bundled_cqlsh_paths(cassandra_home)
    for path in paths:
        if path not in sys.path:
            sys.path.insert(0, path)
    try:
        import cassandra
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.cluster import Cluster
        from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
    except ImportError as exc:
        search_text = ", ".join(searched) if searched else "none"
        raise RuntimeError(
            "Python Cassandra driver is unavailable ({}). Run this script with "
            "the same Python as cqlsh and set CASSANDRA_HOME or "
            "--cassandra-home. Searched cqlsh lib directories: {}. "
            "Only if no bundled driver exists, optionally install a "
            "cassandra-driver version compatible with this Python and "
            "Cassandra release.".format(exc, search_text)
        )
    source = getattr(cassandra, "__file__", "unknown")
    return (
        cassandra,
        PlainTextAuthProvider,
        Cluster,
        DCAwareRoundRobinPolicy,
        TokenAwarePolicy,
        source,
    )


def connect(config):
    connection = config["connection"]
    (
        cassandra_module,
        PlainTextAuthProvider,
        Cluster,
        DCAwareRoundRobinPolicy,
        TokenAwarePolicy,
        driver_source,
    ) = _load_cassandra_driver(connection["cassandra_home"])
    print(
        ">>> Cassandra driver version={} source={}".format(
            getattr(cassandra_module, "__version__", "unknown"), driver_source
        )
    )
    sys.stdout.flush()
    cluster_args = {
        "contact_points": connection["contact_points"],
        "port": connection["port"],
        "connect_timeout": float(connection["connect_timeout_seconds"]),
    }
    from cassandra.policies import FallthroughRetryPolicy
    from cassandra.metadata import murmur3

    cluster_args["default_retry_policy"] = FallthroughRetryPolicy()
    if connection["protocol_version"] is not None:
        cluster_args["protocol_version"] = connection["protocol_version"]
    policy = DCAwareRoundRobinPolicy(local_dc=connection["local_dc"])
    if murmur3 is not None:
        policy = TokenAwarePolicy(policy)
    cluster_args["load_balancing_policy"] = policy
    print(
        ">>> routing={} murmur3={} pid={}".format(
            type(policy).__name__, murmur3 is not None, os.getpid()
        )
    )
    if connection["username_env"]:
        username = os.environ.get(connection["username_env"])
        password = os.environ.get(connection["password_env"])
        if username is None or password is None:
            raise RuntimeError(
                "authentication is configured but environment variable(s) are missing: "
                "{}, {}".format(connection["username_env"], connection["password_env"])
            )
        cluster_args["auth_provider"] = PlainTextAuthProvider(username, password)
    ssl_config = connection["ssl"]
    if ssl_config["enabled"]:
        if not hasattr(ssl, "create_default_context"):
            raise RuntimeError(
                "TLS mode requires Python 2.7.9+ or Python 3.4+; "
                "upgrade the runtime instead of disabling certificate checks"
            )
        context = ssl.create_default_context(cafile=ssl_config["ca_cert"] or None)
        context.check_hostname = ssl_config["check_hostname"]
        context.verify_mode = ssl.CERT_REQUIRED
        if ssl_config["client_cert"]:
            context.load_cert_chain(ssl_config["client_cert"], ssl_config["client_key"])
        cluster_args["ssl_context"] = context
    cluster = Cluster(**cluster_args)
    sessions = []
    try:
        for _ in ITER_RANGE(connection["sessions"]):
            session = cluster.connect()
            session.default_timeout = float(connection["request_timeout_seconds"])
            sessions.append(session)
    except BaseException:
        cluster.shutdown()
        raise
    print(
        ">>> Driver protocol_version={} sessions={} discovered_hosts={} reactor={}".format(
            getattr(cluster, "protocol_version", "unknown"),
            len(sessions),
            len(cluster.metadata.all_hosts()),
            cluster.connection_class.__name__,
        )
    )
    sys.stdout.flush()
    return cluster, sessions


def run_schema_setup(session, config):
    schema = config["schema"]
    if schema["create_keyspace_if_missing"]:
        session.execute(render_ddl(schema["keyspace_cql"], config))
    if schema["create_table_if_missing"]:
        session.execute(render_ddl(schema["table_cql"], config))


def validate_live_schema(session, config):
    schema = config["schema"]
    metadata = session.cluster.metadata
    table = metadata.keyspaces[schema["keyspace"]].tables[schema["table"]]
    partition = [c.name for c in table.partition_key]
    clustering = [c.name for c in table.clustering_key]
    if partition != schema["partition_key_columns"]:
        raise ConfigError(
            "actual partition key {} differs from configured {}".format(
                partition, schema["partition_key_columns"]
            )
        )
    if set(config["verification"]["key_columns"]) != set(partition + clustering):
        raise ConfigError(
            "verification.key_columns must be the complete actual primary key"
        )
    for col in schema["columns"]:
        if col["name"] not in table.columns:
            raise ConfigError(
                "column missing from actual table: {}".format(col["name"])
            )
    if (
        schema["temperature_source"] == "custom_ck"
        and schema["temperature_column"] not in clustering
    ):
        raise ConfigError("temperature_column is not an actual clustering column")
    # Vendor metadata is not standardized. Unknown must be explicit, not reported as verified.
    raw = next(
        iter(
            session.execute(
                "SELECT * FROM system_schema.tables WHERE keyspace_name=%s AND table_name=%s",
                (schema["keyspace"], schema["table"]),
            )
        ),
        None,
    )
    attributes = raw._asdict() if raw is not None else {}
    chs = (
        attributes.get("z06_chs")
        or attributes.get("Z06_CHS")
        or table.options.get("Z06_CHS")
    )
    chs_status = "not_exposed"
    if isinstance(chs, dict):
        match = re.search(
            r"Z06_CHS\s*=\s*\{([^}]*)\}", schema["table_cql"], re.IGNORECASE
        )
        desired = (
            dict(
                re.findall(
                    r"['\"]([a-z_]+)['\"]\s*:\s*['\"]([^'\"]+)['\"]", match.group(1)
                )
            )
            if match
            else {}
        )
        for name, value in desired.items():
            if str(chs.get(name)) != value:
                raise ConfigError(
                    "actual CHS {}={!r} differs from {!r}".format(
                        name, chs.get(name), value
                    )
                )
        actual_column = chs.get("chs_column", "")
        expected_column = schema["temperature_column"]
        if actual_column != expected_column:
            raise ConfigError(
                "actual chs_column {!r} differs from {!r}".format(
                    actual_column, expected_column
                )
            )
        generator = {c["name"]: c["generator"] for c in schema["columns"]}.get(
            expected_column
        )
        expected_unit = {"event_time_seconds": "s", "event_time_millis": "ms"}.get(
            generator
        )
        if expected_unit and chs.get("time_unit") != expected_unit:
            raise ConfigError("CHS time_unit does not match temperature generator")
        chs_status = "verified"
    elif schema["require_chs_metadata"]:
        raise ConfigError(
            "CHS metadata not exposed; cannot verify temperature settings"
        )
    result = {
        "partition_key": partition,
        "clustering_key": clustering,
        "replication": str(metadata.keyspaces[schema["keyspace"]].replication_strategy),
        "chs_metadata": chs_status,
    }
    print(">>> schema_preflight " + json.dumps(result, sort_keys=True))
    sys.stdout.flush()
    return result


def run_optional_truncate(session, config, allow_destructive):
    schema = config["schema"]
    if schema["truncate_before_load"]:
        if not allow_destructive:
            raise RuntimeError(
                "schema.truncate_before_load=true requires the explicit "
                "--allow-destructive flag"
            )
        session.execute("TRUNCATE {}".format(qualified_table(config)))


def _json_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytearray):
        binary_value = str(value) if PY2 else bytes(value)
        return {
            "type": "bytes",
            "size": len(value),
            "sha256": hashlib.sha256(binary_value).hexdigest(),
        }
    return value


def printable_row(row):
    return {key: _json_value(value) for key, value in row.items()}


def config_summary(config):
    workload = config["workload"]
    schema = config["schema"]
    setup_cql = []
    if schema["create_keyspace_if_missing"]:
        setup_cql.append(render_ddl(schema["keyspace_cql"], config))
    if schema["create_table_if_missing"]:
        setup_cql.append(render_ddl(schema["table_cql"], config))
    if schema["truncate_before_load"]:
        setup_cql.append("TRUNCATE {}".format(qualified_table(config)))
    return {
        "target": qualified_table(config),
        "contact_points": config["connection"]["contact_points"],
        "port": config["connection"]["port"],
        "cassandra_home": (
            config["connection"]["cassandra_home"]
            or os.environ.get("CASSANDRA_HOME", "auto/not-set")
        ),
        "sessions": config["connection"]["sessions"],
        "write_mode": workload["write_mode"],
        "concurrency": workload["concurrency"],
        "producer_threads": workload["producer_threads"],
        "processes": workload["processes"],
        "run_id": workload["run_id"],
        "version": VERSION,
        "max_batch_bytes": workload["max_batch_bytes"],
        "window_anchor": workload["window_anchor"],
        "window_sampling": workload["window_sampling"],
        "batch_size": workload["batch_size"],
        "total_rows": workload["total_rows"],
        "duration_seconds": workload["duration_seconds"],
        "rate_limit_rows_per_second": workload["rate_limit_rows_per_second"],
        "vehicle_count": workload["vehicle_count"],
        "temperature_source": schema["temperature_source"],
        "temperature_column": schema["temperature_column"] or None,
        "partition_key_columns": schema["partition_key_columns"],
        "event_time_mode": workload["event_time_mode"],
        "temperature_clock": (
            "cassandra_write_timestamp"
            if schema["temperature_source"] == "write_timestamp"
            else (
                "client_wall_clock_in_{}".format(schema["temperature_column"])
                if workload["event_time_mode"] == "natural_write_time"
                else "configured_weighted_windows_in_{}".format(
                    schema["temperature_column"]
                )
            )
        ),
        "timelines": workload["timelines"],
        "reference_time_utc": workload["reference_time_utc"] or "load_start_utc",
        "time_windows": workload["time_windows"],
        "ttl_seconds": workload["ttl_seconds"],
        "insert_cql": build_insert_cql(config),
        "verification_cql": build_verify_cql(config),
        "schema_setup": {
            "create_keyspace_if_missing": schema["create_keyspace_if_missing"],
            "create_table_if_missing": schema["create_table_if_missing"],
            "truncate_before_load": schema["truncate_before_load"],
            "cql": setup_cql,
        },
    }


def write_summary(path_text, summary):
    if not path_text:
        return
    path = os.path.abspath(path_text)
    parent = os.path.dirname(path)
    if not os.path.isdir(parent):
        raise RuntimeError("summary output directory does not exist: {}".format(parent))
    temporary = path + ".tmp"
    serialized = json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True)
    if isinstance(serialized, TEXT_TYPE):
        serialized = serialized.encode("utf-8")
    with io.open(temporary, "wb") as output:
        output.write(serialized + b"\n")
    replace_file = getattr(os, "replace", os.rename)
    replace_file(temporary, path)


def _database_setup(config, allow_destructive, output):
    cluster = None
    try:
        cluster, sessions = connect(config)
        session = sessions[0]
        run_schema_setup(session, config)
        session.cluster.refresh_schema_metadata()
        prepare_insert(session, config)
        prepare_verification(session, config)
        schema = validate_live_schema(session, config)
        run_optional_truncate(session, config, allow_destructive)
        output.put(("setup", -1, schema))
    except Exception as exc:
        output.put(("error", -1, "{}: {}".format(type(exc).__name__, exc)))
    finally:
        if cluster is not None:
            cluster.shutdown()


def _load_process(
    worker_id, config, vehicle_offset, start, start_event, stop_event, output
):
    cluster = None
    try:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
        cluster, sessions = connect(config)
        prepared = prepare_insert(sessions[0], config)
        verifier = prepare_verification(sessions[0], config)
        generator = VehicleRowGenerator(config, vehicle_offset=vehicle_offset)
        output.put(("ready", worker_id, {"pid": os.getpid()}))
        while not start_event.wait(0.1):
            if stop_event.is_set():
                return
        if stop_event.is_set():
            return

        # Send numeric stats only; never IPC individual generated rows.
        def report(snapshot):
            output.put(("progress", worker_id, snapshot))

        summary, samples = run_load(
            sessions,
            prepared,
            config,
            generator=generator,
            show_progress=False,
            progress_callback=report,
            start_at=start.value,
            external_stop=stop_event,
        )
        if summary["failed_rows"]:
            stop_event.set()
        output.put(("progress", worker_id, summary))
        verification = run_verification(
            sessions[0], config, samples, statement=verifier
        )
        output.put(
            (
                "done",
                worker_id,
                {"load": summary, "verification": verification, "pid": os.getpid()},
            )
        )
    except Exception as exc:
        stop_event.set()
        output.put(("error", worker_id, "{}: {}".format(type(exc).__name__, exc)))
    finally:
        if cluster is not None:
            cluster.shutdown()


def _aggregate_snapshots(snapshots, elapsed):
    elapsed = max(elapsed, 1e-9)
    fields = (
        "claimed_rows",
        "in_flight",
        "succeeded_rows",
        "failed_rows",
        "request_attempts",
        "request_errors",
        "requests_in_flight",
        "completion_queued",
        "retry_attempts",
        "encoded_bytes",
    )
    result = {name: sum(s.get(name, 0) for s in snapshots) for name in fields}
    result["elapsed_seconds"] = elapsed
    result["rows_per_second"] = result["succeeded_rows"] / elapsed
    result["requests_per_second"] = result["request_attempts"] / elapsed
    result["client_cpu_percent"] = (
        sum(s.get("client_cpu_percent", 0) * s["elapsed_seconds"] for s in snapshots)
        / elapsed
    )
    hosts = {}
    weighted_samples = []
    latency_total = 0.0
    queue_total = 0.0
    for snapshot in snapshots:
        for host, count in snapshot.get("coordinator_requests", {}).items():
            hosts[host] = hosts.get(host, 0) + count
        n = snapshot["request_attempts"]
        samples = snapshot.get("_latency_samples", [])
        weight = n / float(max(1, len(samples)))
        weighted_samples.extend((v, weight) for v in samples)
        latency_total += snapshot["latency_ms"]["mean"] * n
        queue_total += snapshot.get("queue_delay_ms_mean", 0) * n
    weighted_samples.sort()
    latency = {
        "sample_count": len(weighted_samples),
        "mean": latency_total / max(1, result["request_attempts"]),
        "max": max([s["latency_ms"]["max"] for s in snapshots] or [0]),
    }
    for name, fraction in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99)):
        threshold = sum(w for _, w in weighted_samples) * fraction
        accumulated = 0.0
        latency[name] = 0.0
        for value, weight in weighted_samples:
            accumulated += weight
            if accumulated >= threshold:
                latency[name] = round(value, 3)
                break
    result["latency_ms"] = latency
    result["queue_delay_ms_mean"] = queue_total / max(1, result["request_attempts"])
    result["coordinator_requests"] = hosts
    result["errors"] = [e for s in snapshots for e in s.get("errors", [])][:10]
    return result


def _interval_progress(summary, previous, config):
    span = summary["elapsed_seconds"] - (previous["elapsed_seconds"] if previous else 0)
    span = max(span, 1e-9)
    rows = summary["succeeded_rows"] - (previous["succeeded_rows"] if previous else 0)
    req = summary["request_attempts"] - (
        previous["request_attempts"] if previous else 0
    )
    encoded = summary["encoded_bytes"] - (previous["encoded_bytes"] if previous else 0)
    print(
        ">>> progress utc={} elapsed={:.1f}s rows={} failed={} row_rate={:.1f}/s avg_row_rate={:.1f}/s "
        "req_rate={:.1f}/s driver_pending={} completed_queued={} retry_attempts={} request_errors={} "
        "p95_all={:.3f}ms callback_queue_mean={:.3f}ms client_cpu_avg={:.1f}% encoded_MBps={:.3f} coordinators={}".format(
            _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            summary["elapsed_seconds"],
            summary["succeeded_rows"],
            summary["failed_rows"],
            rows / span,
            summary["rows_per_second"],
            req / span,
            summary["requests_in_flight"],
            summary["completion_queued"],
            summary["retry_attempts"],
            summary["request_errors"],
            summary["latency_ms"]["p95"],
            summary["queue_delay_ms_mean"],
            summary["client_cpu_percent"],
            encoded / span / 1000000,
            json.dumps(summary["coordinator_requests"], sort_keys=True),
        )
    )
    sys.stdout.flush()


def run_distributed(config, allow_destructive=False):
    # Parent never creates/imports a live driver Cluster. Py2 fork and Py3 spawn are both safe here.
    context = (
        multiprocessing.get_context("spawn")
        if hasattr(multiprocessing, "get_context")
        else multiprocessing
    )
    output = context.Queue()
    stop = context.Event()
    go = context.Event()
    start = context.Value("d", 0.0)
    processes = []
    old_signals = {}
    interrupted = [False]

    def on_signal(*_):
        interrupted[0] = True
        stop.set()
        go.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        old_signals[sig] = signal.signal(sig, on_signal)
    workload = config["workload"]
    try:
        setup = context.Process(
            target=_database_setup, args=(config, allow_destructive, output)
        )
        setup.start()
        processes.append(setup)
        timeout = max(60, config["connection"]["request_timeout_seconds"] * 3)
        kind, _, schema = output.get(timeout=timeout)
        setup.join(timeout)
        if kind != "setup" or setup.exitcode != 0:
            raise RuntimeError("database setup failed: {}".format(schema))
        count = min(
            workload["processes"], workload["concurrency"], workload["vehicle_count"]
        )
        if workload["total_rows"]:
            count = min(count, workload["total_rows"])
        run_id = workload["run_id"]
        if run_id == "auto":
            run_id = uuid.uuid4().hex[:12]
        offset = 0
        children = []
        for index in ITER_RANGE(count):
            child = copy.deepcopy(config)
            w = child["workload"]
            w["concurrency"] = workload["concurrency"] // count + (
                index < workload["concurrency"] % count
            )
            w["producer_threads"] = min(w["producer_threads"], w["concurrency"])
            w["vehicle_count"] = workload["vehicle_count"] // count + (
                index < workload["vehicle_count"] % count
            )
            w["total_rows"] = workload["total_rows"] // count + (
                index < workload["total_rows"] % count
            )
            w["rate_limit_rows_per_second"] = workload[
                "rate_limit_rows_per_second"
            ] / float(count)
            w["progress_interval_seconds"] = min(
                workload["progress_interval_seconds"] or 1, 1
            )
            child["verification"]["sample_size"] = config["verification"][
                "sample_size"
            ] // count + (index < config["verification"]["sample_size"] % count)
            if run_id:
                w["vehicle_id_prefix"] = workload["vehicle_id_prefix"] + run_id + "-"
            process = context.Process(
                target=_load_process,
                args=(index, child, offset, start, go, stop, output),
            )
            offset += w["vehicle_count"]
            process.start()
            children.append(process)
            processes.append(process)
        ready = set()
        ready_deadline = MONOTONIC_TIME() + timeout
        while len(ready) < count:
            if stop.is_set() or MONOTONIC_TIME() > ready_deadline:
                raise RuntimeError("workers did not become ready")
            try:
                kind, index, message = output.get(timeout=0.2)
            except queue_module.Empty:
                if any(p.exitcode is not None for p in children):
                    raise RuntimeError("worker exited during startup")
                continue
            if kind == "error":
                raise RuntimeError("worker {}: {}".format(index, message))
            if kind == "ready":
                ready.add(index)
        print(
            ">>> Loading {} version={} processes={} producers_per_process={} global_concurrency={} batch={} run_id={!r}".format(
                qualified_table(config),
                VERSION,
                count,
                workload["producer_threads"],
                workload["concurrency"],
                workload["batch_size"]
                if workload["write_mode"] == "unlogged_batch"
                else 1,
                run_id,
            )
        )
        sys.stdout.flush()
        start.value = MONOTONIC_TIME()
        go.set()
        latest = {}
        done = {}
        previous = None
        interval = workload["progress_interval_seconds"]
        next_report = start.value + (interval or 1e100)
        stop_at = None
        while len(done) < count:
            now = MONOTONIC_TIME()
            if stop.is_set() and stop_at is None:
                stop_at = now
            if (
                stop_at is not None
                and now - stop_at > workload["drain_timeout_seconds"] + 5
            ):
                raise RuntimeError(
                    "worker shutdown exceeded deadline; writes may have unknown outcome"
                )
            try:
                kind, index, message = output.get(timeout=0.2)
            except queue_module.Empty:
                for index, process in enumerate(children):
                    if process.exitcode is not None and index not in done:
                        raise RuntimeError(
                            "worker {} exited without final report ({})".format(
                                index, process.exitcode
                            )
                        )
                continue
            if kind == "error":
                raise RuntimeError("worker {}: {}".format(index, message))
            if kind == "progress":
                latest[index] = message
            elif kind == "done":
                done[index] = message
                latest[index] = message["load"]
            if MONOTONIC_TIME() >= next_report and latest:
                summary = _aggregate_snapshots(
                    list(latest.values()), MONOTONIC_TIME() - start.value
                )
                _interval_progress(summary, previous, config)
                previous = summary
                next_report = MONOTONIC_TIME() + interval
        elapsed = max(s["elapsed_seconds"] for s in latest.values())
        summary = _aggregate_snapshots(list(latest.values()), elapsed)
        summary.update(
            processes=count,
            configured_concurrency=workload["concurrency"],
            write_mode=workload["write_mode"],
            batch_size=workload["batch_size"]
            if workload["write_mode"] == "unlogged_batch"
            else 1,
            stop_reason="interrupted"
            if interrupted[0]
            else (
                "rows"
                if workload["total_rows"]
                and summary["succeeded_rows"] >= workload["total_rows"]
                else "duration"
            ),
        )
        for item in done.values():
            item["load"].pop("_latency_samples", None)
        verification = {
            name: sum(item["verification"].get(name, 0) for item in done.values())
            for name in ("checked", "found", "missing", "mismatched_values")
        }
        verification["enabled"] = config["verification"]["enabled"]
        status = "PASS"
        if (
            summary["failed_rows"]
            or verification["missing"]
            or verification["mismatched_values"]
        ):
            status = "FAIL"
            summary["stop_reason"] = "error"
        if interrupted[0]:
            status = "INTERRUPTED"
        for process in children:
            process.join(timeout)
            if process.exitcode != 0:
                raise RuntimeError("worker did not exit cleanly")
        return {
            "status": status,
            "version": VERSION,
            "run_id": run_id,
            "target": qualified_table(config),
            "temperature_source": config["schema"]["temperature_source"],
            "event_time_mode": workload["event_time_mode"],
            "schema": schema,
            "load": summary,
            "verification": verification,
            "workers": [done[i] for i in sorted(done)],
            "finished_at_utc": _utc_now().isoformat(),
        }
    finally:
        stop.set()
        go.set()
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(2)
                if process.is_alive():
                    os.kill(process.pid, signal.SIGKILL)
                    process.join(2)
        for sig, handler in old_signals.items():
            signal.signal(sig, handler)
        output.close()


def parse_count(value):
    match = _fullmatch(r"\s*(\d+)\s*([kKmMbB]?)\s*", value)
    if not match:
        raise argparse.ArgumentTypeError(
            "use an integer or k/m/b suffix, for example 500k or 10m"
        )
    multiplier = {"": 1, "k": 1000, "m": 1000000, "b": 1000000000}[
        match.group(2).lower()
    ]
    return int(match.group(1)) * multiplier


def parse_duration(value):
    match = _fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([smhd]?)\s*", value, re.IGNORECASE)
    if not match:
        raise argparse.ArgumentTypeError(
            "use seconds or s/m/h/d suffix, for example 300, 30m, or 2h"
        )
    multiplier = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}[match.group(2).lower()]
    return float(match.group(1)) * multiplier


def build_argument_parser():
    parser = argparse.ArgumentParser(
        description="Write configurable multi-timeline vehicle data to Cassandra.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--config", required=True, help="JSON configuration path")
    parser.add_argument(
        "--processes",
        type=int,
        help="independent writer processes; concurrency/rate/rows stay global",
    )
    parser.add_argument(
        "--run-id",
        help="auto (default), explicit run identity, or empty for intentional replay",
    )
    parser.add_argument(
        "--cassandra-home",
        help="reuse the bundled driver from CASSANDRA_HOME/lib, like cqlsh.py",
    )
    parser.add_argument(
        "--sessions",
        type=int,
        help="driver Session count; each adds one protocol-v3+ connection per host",
    )
    parser.add_argument(
        "--write-mode",
        choices=("async", "unlogged_batch", "sync"),
        help="override write transport mode",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        help="override total in-flight requests (or thread count in sync mode)",
    )
    parser.add_argument(
        "--producer-threads", type=int, help="override async request producer threads"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="rows per same-partition UNLOGGED BATCH",
    )
    parser.add_argument(
        "--vehicle-id-prefix",
        help="override vehicle ID prefix; use a unique prefix per load process",
    )
    parser.add_argument(
        "--rows", type=parse_count, help="override successful row target; 0 disables"
    )
    parser.add_argument(
        "--duration",
        type=parse_duration,
        help="override run duration; 0 disables (earliest limit wins)",
    )
    parser.add_argument(
        "--rate", type=float, help="override aggregate rows/second; 0 is unlimited"
    )
    parser.add_argument(
        "--event-time-mode",
        "--temperature-mode",
        dest="event_time_mode",
        choices=("natural_write_time", "weighted_time_windows"),
        help="override business event-time generation mode",
    )
    parser.add_argument("--summary-json", help="override JSON summary output path")
    parser.add_argument(
        "--allow-destructive",
        action="store_true",
        help="required when config explicitly enables truncate_before_load",
    )
    parser.add_argument(
        "--check-config",
        action="store_true",
        help="validate and print the effective target; do not connect",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help=(
            "run built-in stdlib-only generation and concurrency tests; do not connect"
        ),
    )
    parser.add_argument(
        "--dry-run",
        nargs="?",
        const=5,
        type=int,
        metavar="N",
        help="print N generated rows and CQL; do not connect",
    )
    return parser


def main(argv=None):
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    try:
        config = load_json_config(args.config)
        # Normalize only after overrides, so valid CLI overrides can repair incomplete settings.
        for name in ("connection", "workload", "report"):
            config.setdefault(name, {})
        config = apply_overrides(config, args)
        if args.concurrency is not None and args.concurrency < 1:
            raise ConfigError("--concurrency must be >= 1")
        if args.rate is not None and args.rate < 0:
            raise ConfigError("--rate must be >= 0")
        if args.dry_run is not None and args.dry_run < 1:
            raise ConfigError("--dry-run N must use N >= 1")
    except ConfigError as exc:
        print("CONFIG ERROR: {}".format(exc), file=sys.stderr)
        return EXIT_CONFIG_OR_ENV

    print(json.dumps(config_summary(config), ensure_ascii=True, indent=2))
    sys.stdout.flush()
    if args.self_test:
        result = run_self_test(config)
        print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
        print("PASS: built-in self-test; no Cassandra connection was made")
        return EXIT_OK
    if args.check_config:
        print("PASS: configuration is valid; no Cassandra connection was made")
        return EXIT_OK
    if args.dry_run is not None:
        generator = VehicleRowGenerator(config)
        for index in ITER_RANGE(args.dry_run):
            print(
                json.dumps(
                    printable_row(generator.generate(index)),
                    ensure_ascii=True,
                    sort_keys=True,
                )
            )
        print(
            "PASS: generated {} dry-run row(s); "
            "no Cassandra connection was made".format(args.dry_run)
        )
        return EXIT_OK

    if config["schema"]["truncate_before_load"] and not args.allow_destructive:
        print(
            "CONFIG ERROR: truncate_before_load requires --allow-destructive",
            file=sys.stderr,
        )
        return EXIT_CONFIG_OR_ENV

    try:
        final_summary = run_distributed(config, args.allow_destructive)
        load_summary = final_summary["load"]
        verification = final_summary["verification"]
        write_summary(config["report"]["summary_json"], final_summary)
        print(json.dumps(final_summary, ensure_ascii=True, indent=2))
        sys.stdout.flush()
        if final_summary["status"] == "PASS":
            print(
                "PASS: wrote {} row(s); verified {}/{} sample(s)".format(
                    load_summary["succeeded_rows"],
                    verification["found"],
                    verification["checked"],
                )
            )
            return EXIT_OK
        if final_summary["status"] == "INTERRUPTED":
            return EXIT_INTERRUPTED
        print("FAIL: load or read-back verification reported errors", file=sys.stderr)
        return EXIT_LOAD_FAILED
    except KeyboardInterrupt:
        return EXIT_INTERRUPTED
    except (ConfigError, RuntimeError) as exc:
        write_summary(
            config["report"]["summary_json"],
            {
                "status": "FAIL",
                "version": VERSION,
                "error": str(exc),
                "writes_may_have_unknown_outcome": True,
            },
        )
        print("ERROR: {}".format(exc), file=sys.stderr)
        return EXIT_CONFIG_OR_ENV
    except Exception as exc:  # noqa: BLE001 - convert all top-level failures to exit codes.
        write_summary(
            config["report"]["summary_json"],
            {
                "status": "FAIL",
                "version": VERSION,
                "error": "{}: {}".format(type(exc).__name__, exc),
                "writes_may_have_unknown_outcome": True,
            },
        )
        print("FAIL: {}: {}".format(type(exc).__name__, exc), file=sys.stderr)
        return EXIT_LOAD_FAILED


if __name__ == "__main__":
    sys.exit(main())
