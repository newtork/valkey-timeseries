import pytest
from valkey import ResponseError

from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesIngest(ValkeyTimeSeriesTestCaseBase):
    def get_sample(self, key, timestamp):
        res = self.client.execute_command("TS.RANGE", key, '-', '+', "FILTER_BY_TS", timestamp)
        assert len(res) == 1, f"Expected one sample at timestamp {timestamp} in series {key}, got: {res}"
        return res[0]

    def test_basic_returns_success_and_total(self):
        key = "series_addbulk_basic"
        payload = r'{"values":[1,2,3],"timestamps":[1000,2000,3000]}'

        res = self.client.execute_command("TS.ADDBULK", key, payload)
        assert res == [3, 3], f"Expected ingestion to succeed with total 3, got: {res}"

        # Verify the samples exist and are retrievable.
        assert self.get_sample(key, 1000) == [1000, b"1"]
        assert self.get_sample(key, 2000) == [2000, b"2"]
        assert self.get_sample(key, 3000) == [3000, b"3"]

    def test_sorts_samples_by_timestamp(self):
        client = self.client
        key = "series_addbulk_basic"
        payload = r'{"values":[3,1,2],"timestamps":[3000,1000,2000]}'

        res = client.execute_command("TS.ADDBULK", key, payload)
        assert res == [3, 3]

        assert self.get_sample(key, 1000) == [1000, b"1"]
        assert self.get_sample(key, 2000) == [2000, b"2"]
        assert self.get_sample(key, 3000) == [3000, b"3"]

    def test_rejects_mismatched_array_lengths(self):
        payload = r'{"values":[1,2],"timestamps":[1000]}'

        with pytest.raises(Exception) as excinfo:
            self.client.execute_command("TS.ADDBULK", "series_addbulk_bad_lengths", payload)

        assert "length mismatch" in str(excinfo.value).lower()

    def test_requires_values(self):
        payload = r'{"timestamps":[1000,2000]}'

        with pytest.raises(Exception) as excinfo:
            self.client.execute_command("TS.ADDBULK", "series_addbulk_missing_values", payload)

        assert "missing values" in str(excinfo.value).lower()

    def test_ingest_requires_timestamps(self):
        client = self.server.get_new_client()
        payload = r'{"values":[1,2]}'

        with pytest.raises(Exception) as excinfo:
            client.execute_command("TS.ADDBULK", "series_addbulk_missing_timestamps", payload)

        assert "missing timestamps" in str(excinfo.value).lower()

    def test_ingest_rejects_empty_arrays(self):
        with pytest.raises(Exception) as excinfo1:
            self.client.execute_command("TS.ADDBULK", "series_addbulk_empty_1", r'{"values":[],"timestamps":[1]}')
        assert "no timestamps or values" in str(excinfo1.value).lower()

        with pytest.raises(Exception) as excinfo2:
            self.client.execute_command("TS.ADDBULK", "series_addbulk_empty_2", r'{"values":[1],"timestamps":[]}')
        assert "no timestamps or values" in str(excinfo2.value).lower()

    def test_invalid_json_returns_error(self):
        payload = r'{"values":[1,2],"timestamps":[1000,2000]'  # missing closing brace

        with pytest.raises(Exception):
            self.client.execute_command("TS.ADDBULK", "series_addbulk_invalid_json", payload)

    def test_non_numeric_entries_raise_error(self):
        # Non-numeric value "x" will be dropped, causing values.len != timestamps.len -> error.
        payload = r'{"values":[1,"x",3],"timestamps":[1000,2000,3000]}'

        with pytest.raises(Exception) as excinfo:
            self.client.execute_command("TS.ADDBULK", "series_addbulk_non_numeric", payload)

        assert "invalid value" in str(excinfo.value).lower() or "length mismatch" in str(excinfo.value).lower()

    def test_runs_compactions_and_writes_destination_series(self):
        src = "series_addbulk_compact_src"
        dest = "series_addbulk_compact_dest"

        # Create source and destination series, then attach a compaction rule (bucket=10).
        self.client.execute_command("TS.CREATE", src)
        self.client.execute_command("TS.CREATE", dest)
        self.client.execute_command("TS.CREATERULE", src, dest, "AGGREGATION", "SUM", 10)

        # Ingest 3 samples that fall into 2 buckets: [0..9] and [10..19].
        # Expected SUMs: bucket 0 -> 1+2=3, bucket 10 -> 3
        payload = r'{"values":[1,2,3],"timestamps":[1,2,11]}'
        res = self.client.execute_command("TS.ADDBULK", src, payload)
        assert res == [3, 3]

        # Verify compaction destination series received aggregated samples.
        assert self.get_sample(dest, 0) == [0, b"3"]
        assert self.get_sample(dest, 10) == [10, b"3"]

    def test_runs_nested_compactions(self):
        src = "series_addbulk_nested_compact_src"
        mid = "series_addbulk_nested_compact_mid"
        dest = "series_addbulk_nested_compact_dest"

        # Create series and chain compaction rules:
        # src --SUM(10)--> mid --SUM(20)--> dest
        self.client.execute_command("TS.CREATE", src)
        self.client.execute_command("TS.CREATE", mid)
        self.client.execute_command("TS.CREATE", dest)

        self.client.execute_command("TS.CREATERULE", src, mid, "AGGREGATION", "SUM", 10)
        self.client.execute_command("TS.CREATERULE", mid, dest, "AGGREGATION", "SUM", 20)

        # Ingest samples that span two 10-sized buckets:
        # bucket 0: ts=1,2 => 1+2=3
        # bucket 10: ts=11 => 3
        payload = r'{"values":[1,2,3],"timestamps":[1,2,11]}'
        res = self.client.execute_command("TS.ADDBULK", src, payload)
        assert res == [3, 3]

        # First-level compaction results in mid.
        assert self.get_sample(mid, 0) == [0, b"3"]
        assert self.get_sample(mid, 10) == [10, b"3"]

        # Second-level compaction results in dest (20-sized bucket):
        # bucket 0: mid(0)=3 + mid(10)=3 => 6
        assert self.get_sample(dest, 0) == [0, b"6"]

    def test_large_batch_runs_multi_level_compactions(self):
        src = "series_addbulk_large_compact_src"
        l1 = "series_addbulk_large_compact_l1"
        l2 = "series_addbulk_large_compact_l2"
        l3 = "series_addbulk_large_compact_l3"

        # Create the series and a 3-level compaction chain:
        # src --SUM(10)--> l1 --SUM(50)--> l2 --SUM(100)--> l3
        self.client.execute_command("TS.CREATE", src)
        self.client.execute_command("TS.CREATE", l1)
        self.client.execute_command("TS.CREATE", l2)
        self.client.execute_command("TS.CREATE", l3)

        self.client.execute_command("TS.CREATERULE", src, l1, "AGGREGATION", "SUM", 10)
        self.client.execute_command("TS.CREATERULE", l1, l2, "AGGREGATION", "SUM", 50)
        self.client.execute_command("TS.CREATERULE", l2, l3, "AGGREGATION", "SUM", 100)

        # Large ingestion: timestamps 0..999, all values are 1.
        # This creates predictable sums per bucket at each level.
        n = 1000
        timestamps = list(range(n))
        values = [1] * n

        payload = f'{{"values":{values},"timestamps":{timestamps}}}'
        res = self.client.execute_command("TS.ADDBULK", src, payload)
        assert res == [n, n]

        # Level 1: bucket=10, each bucket sum is 10.
        assert self.get_sample(l1, 0) == [0, b"10"]
        assert self.get_sample(l1, 900) == [900, b"10"]

        # Level 2: bucket=50, sums over five l1 buckets => 50.
        assert self.get_sample(l2, 0) == [0, b"50"]
        assert self.get_sample(l2, 950) == [950, b"50"]

        # Level 3: bucket=100, sums over two l2 buckets => 100.
        assert self.get_sample(l3, 0) == [0, b"100"]
        assert self.get_sample(l3, 900) == [900, b"100"]
