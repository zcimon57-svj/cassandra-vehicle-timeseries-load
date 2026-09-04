#!/usr/bin/env python

from __future__ import print_function

import copy
import io
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
load_tool = __import__("cassandra_vehicle_timeseries_load")


class FakeSession:
    def __init__(self, fail_first_request=False):
        self.lock = threading.Lock()
        self.rows = []
        self.statements = []
        self.execute_times = []
        self.fail_first_request = fail_first_request
        self.calls = 0

    def execute(self, _prepared, values=None, timeout=None):
        del timeout
        with self.lock:
            self.calls += 1
            self.statements.append(_prepared)
            self.execute_times.append(time.time())
            if self.fail_first_request and self.calls == 1:
                raise RuntimeError("synthetic transient error")
            self.rows.append(values)


class VehicleTimeseriesLoadTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load_tool.validate_config(
            load_tool.load_json_config(os.path.join(HERE, "example-config.json"))
        )

    def small_config(self, rows=100):
        config = copy.deepcopy(self.config)
        config["schema"]["create_keyspace_if_missing"] = False
        config["schema"]["create_table_if_missing"] = False
        config["schema"]["temperature_source"] = "custom_ck"
        config["schema"]["temperature_column"] = "event_time_s"
        config["workload"].update(
            {
                "concurrency": 8,
                "total_rows": rows,
                "duration_seconds": 0,
                "rate_limit_rows_per_second": 0,
                "vehicle_count": 2,
                "event_time_mode": "weighted_time_windows",
                "reference_time_utc": "2026-09-01T00:00:00Z",
                "time_windows": [
                    {
                        "name": "test",
                        "weight": 1,
                        "start_offset_seconds": 0,
                        "end_offset_seconds": 3600,
                    }
                ],
                "progress_interval_seconds": 0,
            }
        )
        config["verification"]["enabled"] = False
        return load_tool.validate_config(config)

    def test_example_config_and_cql_are_valid(self):
        cql = load_tool.build_insert_cql(self.config)
        self.assertIn('INSERT INTO "chs_load_test"."vehicle_timeseries"', cql)
        self.assertNotIn("USING TTL", cql)
        self.assertIn("Z06_CHS", self.config["schema"]["table_cql"])
        self.assertNotIn("chs_column", self.config["schema"]["table_cql"])
        self.assertEqual(self.config["schema"]["temperature_source"], "write_timestamp")
        self.assertEqual(
            self.config["workload"]["event_time_mode"], "natural_write_time"
        )

    def test_each_vehicle_gets_multiple_timelines_and_monotonic_stream_time(self):
        config = self.small_config()
        generator = load_tool.VehicleRowGenerator(
            config, now_utc=datetime(2026, 9, 3, tzinfo=load_tool.UTC_TZ)
        )
        rows = [generator.generate(index) for index in range(8)]
        self.assertEqual(
            [row["timeline"] for row in rows[:3]], ["gps", "powertrain", "battery"]
        )
        self.assertEqual({row["vehicle_id"] for row in rows[:3]}, {"vehicle-00000000"})
        self.assertEqual({row["vehicle_id"] for row in rows[3:6]}, {"vehicle-00000001"})
        self.assertEqual(rows[0]["event_time_s"], 1788220800)
        self.assertEqual(rows[6]["event_time_s"], 1788220805)
        self.assertEqual(rows[7]["event_time_s"], 1788220801)
        self.assertEqual(rows[6]["sample_seq"], 6)

    def test_weighted_cold_and_hot_windows_stay_in_bounds(self):
        config = copy.deepcopy(self.config)
        config["schema"]["temperature_source"] = "custom_ck"
        config["schema"]["temperature_column"] = "event_time_s"
        config["schema"]["create_table_if_missing"] = False
        config["workload"].update(
            {
                "vehicle_count": 1,
                "event_time_mode": "weighted_time_windows",
                "reference_time_utc": "2026-09-01T00:00:00Z",
                "timestamp_jitter_seconds": 0,
            }
        )
        config = load_tool.validate_config(config)
        generator = load_tool.VehicleRowGenerator(config)
        rows = [generator.generate(index) for index in range(15)]
        self.assertEqual(
            [row["time_window"] for row in rows], ["cold"] * 12 + ["hot"] * 3
        )
        reference = 1788220800
        for row in rows:
            if row["time_window"] == "cold":
                self.assertGreaterEqual(row["event_time_s"], reference - 604800)
                self.assertLessEqual(row["event_time_s"], reference - 86400)
            else:
                self.assertGreaterEqual(row["event_time_s"], reference - 300)
                self.assertLessEqual(row["event_time_s"], reference)

    def test_natural_mode_uses_generation_time_as_temperature(self):
        config = self.small_config()
        config["workload"]["event_time_mode"] = "natural_write_time"
        config["workload"]["time_windows"] = []
        config = load_tool.validate_config(config)
        generator = load_tool.VehicleRowGenerator(config)
        row = generator.generate(7, now_epoch_seconds=1788220999.875)
        self.assertEqual(row["event_time_s"], 1788220999)
        self.assertEqual(row["time_window"], "natural")
        self.assertEqual(row["sample_seq"], 7)

    def test_write_timestamp_source_rejects_artificial_temperature_windows(self):
        config = copy.deepcopy(self.config)
        config["workload"]["event_time_mode"] = "weighted_time_windows"
        with self.assertRaises(load_tool.ConfigError):
            load_tool.validate_config(config)

        config = copy.deepcopy(self.config)
        config["schema"]["table_cql"] = config["schema"]["table_cql"].replace(
            "{'chs_time'", "{'chs_column':'event_time_s','chs_time'"
        )
        with self.assertRaises(load_tool.ConfigError):
            load_tool.validate_config(config)

    def test_custom_ck_ddl_requires_matching_chs_column(self):
        config = copy.deepcopy(self.config)
        config["schema"]["temperature_source"] = "custom_ck"
        config["schema"]["temperature_column"] = "event_time_s"
        with self.assertRaises(load_tool.ConfigError):
            load_tool.validate_config(config)
        config["schema"]["table_cql"] = config["schema"]["table_cql"].replace(
            "{'chs_time'", "{'chs_column':'event_time_s','chs_time'"
        )
        validated = load_tool.validate_config(config)
        self.assertEqual(validated["schema"]["temperature_column"], "event_time_s")

    def test_natural_temperature_is_captured_immediately_before_execute(self):
        config = self.small_config(rows=25)
        config["workload"]["event_time_mode"] = "natural_write_time"
        config["workload"]["time_windows"] = []
        config = load_tool.validate_config(config)
        session = FakeSession()
        summary, _ = load_tool.run_load(
            session,
            prepared=object(),
            config=config,
            generator=load_tool.VehicleRowGenerator(config),
            show_progress=False,
        )
        event_index = [column["name"] for column in config["schema"]["columns"]].index(
            "event_time_s"
        )
        window_index = [column["name"] for column in config["schema"]["columns"]].index(
            "time_window"
        )
        self.assertEqual(summary["succeeded_rows"], 25)
        for values, execute_time in zip(session.rows, session.execute_times):
            self.assertLessEqual(abs(int(execute_time) - values[event_index]), 1)
            self.assertEqual(values[window_index], "natural")

    def test_generation_is_deterministic_and_has_no_nulls(self):
        config = self.small_config()
        first = load_tool.VehicleRowGenerator(config).generate(42)
        second = load_tool.VehicleRowGenerator(config).generate(42)
        self.assertEqual(first, second)
        self.assertTrue(all(value is not None for value in first.values()))
        self.assertEqual(len(first["payload"]), 256)

    def test_ttl_adds_one_bind_marker_and_value(self):
        config = self.small_config()
        config["workload"]["ttl_seconds"] = 60
        config = load_tool.validate_config(config)
        generator = load_tool.VehicleRowGenerator(config)
        row = generator.generate(0)
        self.assertTrue(load_tool.build_insert_cql(config).endswith("USING TTL ?"))
        self.assertEqual(generator.bind_values(row)[-1], 60)

    def test_concurrent_load_hits_exact_success_target(self):
        config = self.small_config(rows=257)
        session = FakeSession()
        summary, verify_rows = load_tool.run_load(
            session,
            prepared=object(),
            config=config,
            generator=load_tool.VehicleRowGenerator(config),
            show_progress=False,
        )
        self.assertEqual(summary["succeeded_rows"], 257)
        self.assertEqual(summary["failed_rows"], 0)
        self.assertEqual(len(session.rows), 257)
        self.assertEqual(summary["stop_reason"], "rows")
        self.assertLessEqual(len(verify_rows), config["verification"]["sample_size"])

    def test_transient_failure_is_retried(self):
        config = self.small_config(rows=31)
        session = FakeSession(fail_first_request=True)
        summary, _ = load_tool.run_load(
            session,
            prepared=object(),
            config=config,
            generator=load_tool.VehicleRowGenerator(config),
            show_progress=False,
        )
        self.assertEqual(summary["succeeded_rows"], 31)
        self.assertEqual(summary["failed_rows"], 0)
        self.assertEqual(summary["request_errors"], 1)
        self.assertEqual(session.calls, 32)

    def test_duration_limit_stops_rate_limited_workers_cleanly(self):
        config = self.small_config(rows=1)
        config["workload"].update(
            {
                "total_rows": 0,
                "duration_seconds": 0.05,
                "rate_limit_rows_per_second": 200,
            }
        )
        config = load_tool.validate_config(config)
        session = FakeSession()
        summary, _ = load_tool.run_load(
            session,
            prepared=object(),
            config=config,
            generator=load_tool.VehicleRowGenerator(config),
            show_progress=False,
        )
        self.assertEqual(summary["stop_reason"], "duration")
        self.assertEqual(summary["in_flight"], 0)
        self.assertGreater(summary["succeeded_rows"], 0)
        self.assertLess(summary["succeeded_rows"], 100)

    def test_invalid_or_tombstone_prone_config_is_rejected(self):
        config = self.small_config()
        config["schema"]["columns"].append(
            {"name": "bad_null", "generator": "constant", "value": None}
        )
        with self.assertRaises(load_tool.ConfigError):
            load_tool.validate_config(config)

        config = self.small_config()
        config["workload"]["time_windows"] = []
        with self.assertRaises(load_tool.ConfigError):
            load_tool.validate_config(config)

    def test_truncate_is_separate_and_requires_explicit_flag(self):
        config = copy.deepcopy(self.config)
        config["schema"]["truncate_before_load"] = True
        config = load_tool.validate_config(config)
        session = FakeSession()
        load_tool.run_schema_setup(session, config)
        self.assertEqual(session.calls, 2)
        self.assertFalse(
            any(
                str(statement).startswith("TRUNCATE")
                for statement in session.statements
            )
        )
        with self.assertRaises(RuntimeError):
            load_tool.run_optional_truncate(session, config, allow_destructive=False)
        load_tool.run_optional_truncate(session, config, allow_destructive=True)
        self.assertTrue(str(session.statements[-1]).startswith("TRUNCATE"))

    def test_human_cli_parsers(self):
        self.assertEqual(load_tool.parse_count("10m"), 10000000)
        self.assertEqual(load_tool.parse_duration("30m"), 1800)

    def test_summary_json_is_written_in_python2_and_python3(self):
        output_dir = tempfile.mkdtemp(prefix="cassandra-load-summary-")
        output_path = os.path.join(output_dir, "summary.json")
        try:
            load_tool.write_summary(
                output_path, {"status": "PASS", "text": "\u8f66\u8f86"}
            )
            with io.open(output_path, "r", encoding="utf-8") as summary_file:
                value = json.load(summary_file)
            self.assertEqual(value["status"], "PASS")
            self.assertEqual(value["text"], "\u8f66\u8f86")
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)
            os.rmdir(output_dir)


if __name__ == "__main__":
    unittest.main(verbosity=2)
