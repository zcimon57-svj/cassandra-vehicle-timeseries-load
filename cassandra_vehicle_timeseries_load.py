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
import io
import json
import math
import os
import random
import re
import ssl
import sys
import threading
import time
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
    "random_blob": {"size", "pool_size"},
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
            "truncate_before_load",
            "columns",
        },
        "schema",
    )
    _validate_identifier(schema.get("keyspace"), "schema.keyspace")
    _validate_identifier(schema.get("table"), "schema.table")
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
        workload.setdefault("producer_threads", 8),
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
    _require_int(workload.setdefault("max_retries", 2), "workload.max_retries", 0)
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
    selected = ", ".join(quote_identifier(name) for name in keys)
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

    def __init__(self, config, now_utc=None):
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
        self.jitter_seconds = float(workload["timestamp_jitter_seconds"])
        self.blob_cache = {}
        self.blob_cache_lock = threading.Lock()

    def context(self, index, now_epoch_seconds=None):
        vehicle_index = (index // self.rows_per_vehicle) % self.vehicle_count
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
        window_start = self.reference_epoch_seconds + float(
            window["start_offset_seconds"]
        )
        window_end = self.reference_epoch_seconds + float(window["end_offset_seconds"])
        window_span = window_end - window_start
        progression = stream_sequence * float(timeline["interval_seconds"])
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
        rng = random.Random(self._row_seed(index))
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
        values = [row[column["name"]] for column in self.config["schema"]["columns"]]
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
        self.verification_sample_size = verification["sample_size"]
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

    def record_request(self, latency_ms, failed, coordinator=None):
        with self.lock:
            self.requests_in_flight -= 1
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
            for row in rows:
                self.verify_rows.append(
                    {name: row[name] for name in self.verification_keys}
                )
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
                "rows_per_second": self.succeeded / elapsed,
                "requests_per_second": self.request_attempts / elapsed,
                "client_cpu_percent": client_cpu_percent,
                "coordinator_requests": dict(self.coordinator_requests),
                "latency_ms": {
                    "sample_count": len(latencies),
                    "p50": _percentile(latencies, 0.50),
                    "p95": _percentile(latencies, 0.95),
                    "p99": _percentile(latencies, 0.99),
                    "max": latencies[-1] if latencies else 0.0,
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
    def __init__(self, started_at, completed_queue):
        self.started_at = started_at
        self.completed_at = None
        self.error = None
        self.request = None
        self.future = None
        self.coordinator = None
        self.completed_queue = completed_queue

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
        self.completed_queue.put(self)

    def on_error(self, error):
        self.error = error
        self.capture_coordinator()
        self.completed_at = MONOTONIC_TIME()
        self.completed_queue.put(self)


def _make_async_statement(prepared, config, value_rows):
    if config["workload"]["write_mode"] != "unlogged_batch":
        return prepared, value_rows[0]
    from cassandra.query import BatchStatement, BatchType

    statement = BatchStatement(
        batch_type=BatchType.UNLOGGED,
        consistency_level=_consistency_value(config["workload"]["consistency_level"]),
    )
    for values in value_rows:
        statement.add(prepared, values)
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
    completion = _AsyncCompletion(MONOTONIC_TIME(), completed_queue)
    request = {
        "rows": rows,
        "value_rows": value_rows,
        "attempt": attempt,
        "completion": completion,
    }
    completion.request = request
    controller.begin_request()
    try:
        statement, parameters = _make_async_statement(prepared, config, value_rows)
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
    max_attempts = workload["max_retries"] + 1
    completed_queue = queue_module.Queue()
    outstanding = 0
    exhausted = False

    while outstanding or not exhausted:
        while outstanding < request_window and not exhausted:
            logical_indexes = controller.claim_many(
                rows_per_request,
                stay_within_group=(write_mode == "unlogged_batch"),
            )
            if not logical_indexes:
                exhausted = True
                break
            if not controller.wait_for_rate(logical_indexes[-1]):
                controller.cancel_claims(len(logical_indexes))
                exhausted = True
                break
            if write_mode == "unlogged_batch":
                row_indexes = [
                    generator.same_partition_index(index, workload["batch_size"])
                    for index in logical_indexes
                ]
            else:
                row_indexes = logical_indexes
            rows = [generator.generate(index) for index in row_indexes]
            value_rows = [generator.bind_values(row) for row in rows]
            _submit_async_request(
                session,
                prepared,
                config,
                rows,
                value_rows,
                attempt=1,
                controller=controller,
                completed_queue=completed_queue,
            )
            outstanding += 1

        if not outstanding:
            continue
        completion = completed_queue.get()
        outstanding -= 1
        request = completion.request
        latency_ms = (completion.completed_at - completion.started_at) * 1000
        if completion.error is None:
            controller.record_request(
                latency_ms, failed=False, coordinator=completion.coordinator
            )
            controller.complete_success_many(request["rows"])
            continue

        controller.record_request(
            latency_ms, failed=True, coordinator=completion.coordinator
        )
        if request["attempt"] < max_attempts and not controller.stop_event.is_set():
            backoff = float(workload["retry_backoff_seconds"]) * (
                2 ** (request["attempt"] - 1)
            )
            controller.stop_event.wait(backoff)
            if not controller.stop_event.is_set():
                _submit_async_request(
                    session,
                    prepared,
                    config,
                    request["rows"],
                    request["value_rows"],
                    attempt=request["attempt"] + 1,
                    controller=controller,
                    completed_queue=completed_queue,
                )
                outstanding += 1
                continue
        controller.complete_failure_many(completion.error, len(request["rows"]))


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


def _run_worker_specs(worker_specs, controller, config, show_progress):
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
            timeout = 0.5
            if progress_interval > 0:
                timeout = max(0.05, min(0.5, next_progress - MONOTONIC_TIME()))
            all_workers_done.wait(timeout)
            if (
                show_progress
                and progress_interval > 0
                and MONOTONIC_TIME() >= next_progress
            ):
                print_progress(controller.snapshot(), config)
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


def run_load(session, prepared, config, generator=None, show_progress=True):
    generator = generator or VehicleRowGenerator(config)
    controller = LoadController(config)
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
                        sessions[producer_index % len(sessions)],
                        prepared,
                        config,
                        generator,
                        controller,
                        request_window,
                    ),
                )
            )
    _run_worker_specs(worker_specs, controller, config, show_progress)
    summary = controller.snapshot()
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
                return _SelfTestFuture(RuntimeError("synthetic async failure"))
            self.rows.append(values)
        return _SelfTestFuture()


def run_self_test(config):
    test_config = copy.deepcopy(config)
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
        statement.is_idempotent = True
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
    timeout = float(config["connection"]["request_timeout_seconds"])
    for row in sample_rows:
        values = tuple(row[name] for name in keys)
        result = session.execute(statement, values, timeout=timeout).one()
        if result is None:
            missing_keys.append({name: _json_value(row[name]) for name in keys})
        else:
            found += 1
    return {
        "enabled": True,
        "checked": len(sample_rows),
        "found": found,
        "missing": len(missing_keys),
        "missing_keys": missing_keys[:10],
    }


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
        driver_zip = max(driver_zips)
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
    source = paths[0] if paths else "existing Python import path"
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
    if connection["protocol_version"] is not None:
        cluster_args["protocol_version"] = connection["protocol_version"]
    if connection["local_dc"]:
        cluster_args["load_balancing_policy"] = TokenAwarePolicy(
            DCAwareRoundRobinPolicy(local_dc=connection["local_dc"])
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
        if not context.check_hostname:
            context.verify_mode = (
                ssl.CERT_REQUIRED if ssl_config["ca_cert"] else ssl.CERT_NONE
            )
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
        ">>> Driver protocol_version={} sessions={} discovered_hosts={}".format(
            getattr(cluster, "protocol_version", "unknown"),
            len(sessions),
            len(cluster.metadata.all_hosts()),
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
        config = validate_config(load_json_config(args.config))
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

    cluster = None
    try:
        print(
            ">>> Connecting to "
            + ",".join(config["connection"]["contact_points"])
            + ":{}".format(config["connection"]["port"])
        )
        sys.stdout.flush()
        cluster, sessions = connect(config)
        session = sessions[0]
        run_schema_setup(session, config)
        prepared = prepare_insert(session, config)
        verification_prepared = prepare_verification(session, config)
        run_optional_truncate(session, config, args.allow_destructive)
        print(
            ">>> Loading {} mode={} concurrency={} producers={} batch={} "
            "temperature_source={} event_time_mode={}".format(
                qualified_table(config),
                config["workload"]["write_mode"],
                config["workload"]["concurrency"],
                config["workload"]["producer_threads"],
                (
                    config["workload"]["batch_size"]
                    if config["workload"]["write_mode"] == "unlogged_batch"
                    else 1
                ),
                config["schema"]["temperature_source"],
                config["workload"]["event_time_mode"],
            )
        )
        sys.stdout.flush()
        load_summary, sample_rows = run_load(sessions, prepared, config)
        verification = run_verification(
            session, config, sample_rows, statement=verification_prepared
        )
        final_summary = {
            "status": "PASS",
            "target": qualified_table(config),
            "temperature_source": config["schema"]["temperature_source"],
            "event_time_mode": config["workload"]["event_time_mode"],
            "load": load_summary,
            "verification": verification,
            "finished_at_utc": _utc_now().isoformat(),
        }
        if load_summary["failed_rows"] > 0 or verification.get("missing", 0) > 0:
            final_summary["status"] = "FAIL"
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
        print("FAIL: load or read-back verification reported errors", file=sys.stderr)
        return EXIT_LOAD_FAILED
    except KeyboardInterrupt:
        return EXIT_INTERRUPTED
    except (ConfigError, RuntimeError) as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        return EXIT_CONFIG_OR_ENV
    except Exception as exc:  # noqa: BLE001 - convert all top-level failures to exit codes.
        print("FAIL: {}: {}".format(type(exc).__name__, exc), file=sys.stderr)
        return EXIT_LOAD_FAILED
    finally:
        if cluster is not None:
            cluster.shutdown()


if __name__ == "__main__":
    sys.exit(main())
