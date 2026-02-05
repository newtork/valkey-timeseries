import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesMaddBulk(ValkeyTimeSeriesTestCaseBase):

    def test_maddbulk_basic_single_series(self):
        """Test basic bulk ingestion for a single series"""
        key = "series_maddbulk_basic"
        payload = r'{"values":[1,2,3],"timestamps":[1000,2000,3000]}'

        self.client.execute_command("TS.CREATE", key)
        res = self.client.execute_command("TS.MADDBULK", key, payload)
        assert res == [[3, 3]], f"Expected [[3, 3]], got: {res}"

        # Verify samples were inserted
        range_res = self.client.execute_command("TS.RANGE", key, "-", "+")
        assert len(range_res) == 3
        assert range_res[0] == [1000, b"1"]
        assert range_res[1] == [2000, b"2"]
        assert range_res[2] == [3000, b"3"]

    def test_maddbulk_multiple_series(self):
        """Test bulk ingestion for multiple series"""
        key1 = "series_maddbulk_multi_1"
        key2 = "series_maddbulk_multi_2"
        payload1 = r'{"values":[1,2],"timestamps":[1000,2000]}'
        payload2 = r'{"values":[10,20,30],"timestamps":[1000,2000,3000]}'

        self.client.execute_command("TS.CREATE", key1)
        self.client.execute_command("TS.CREATE", key2)
        res = self.client.execute_command("TS.MADDBULK", key1, payload1, key2, payload2)
        assert res == [[2, 2], [3, 3]], f"Expected [[2, 2], [3, 3]], got: {res}"

        # Verify first series
        range1 = self.client.execute_command("TS.RANGE", key1, "-", "+")
        assert len(range1) == 2

        # Verify second series
        range2 = self.client.execute_command("TS.RANGE", key2, "-", "+")
        assert len(range2) == 3

    def test_maddbulk_wrong_arity(self):
        """Test error handling for the wrong number of arguments"""
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command("TS.MADDBULK", "only_one_arg")

        assert "wrong number of arguments" in str(excinfo.value).lower()

    def test_maddbulk_odd_number_of_args(self):
        """Test error when providing an odd number of arguments (missing payload)"""
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command("TS.MADDBULK", "key1", "payload1", "key2")

        assert "wrong number of arguments" in str(excinfo.value).lower()

    def test_maddbulk_invalid_json(self):
        """Test error handling for malformed JSON payload"""
        key = "series_maddbulk_bad_json"
        payload = r'{"values":[1,2],"timestamps":[1000,2000]'  # missing closing brace

        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command("TS.MADDBULK", key, payload)

        # Error should occur and no subsequent series should be processed
        assert self.client.execute_command("EXISTS", key) == 0

    def test_maddbulk_length_mismatch(self):
        """Test error when values and timestamps arrays have different lengths"""
        key = "series_maddbulk_mismatch"
        payload = r'{"values":[1,2,3],"timestamps":[1000,2000]}'

        self.client.execute_command("TS.CREATE", key)
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command("TS.MADDBULK", key, payload)

        assert "length mismatch" in str(excinfo.value).lower() or "array" in str(excinfo.value).lower()

    def test_maddbulk_missing_timestamps(self):
        """Test error when the timestamps field is missing"""
        key = "series_maddbulk_no_ts"
        payload = r'{"values":[1,2,3]}'

        self.client.execute_command("TS.CREATE", key)
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command("TS.MADDBULK", key, payload)

        assert "missing" in str(excinfo.value).lower() or "timestamp" in str(excinfo.value).lower()

    def test_maddbulk_missing_values(self):
        """Test error when the values field is missing"""
        key = "series_maddbulk_no_vals"
        payload = r'{"timestamps":[1000,2000,3000]}'

        self.client.execute_command("TS.CREATE", key)
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command("TS.MADDBULK", key, payload)

        assert "missing" in str(excinfo.value).lower() or "value" in str(excinfo.value).lower()

    def test_maddbulk_empty_arrays(self):
        """Test error when arrays are empty"""
        key = "series_maddbulk_empty"
        payload = r'{"values":[],"timestamps":[]}'

        self.client.execute_command("TS.CREATE", key)
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command("TS.MADDBULK", key, payload)

        assert "no timestamps" in str(excinfo.value).lower() or "empty" in str(excinfo.value).lower()

    def test_maddbulk_partial_success(self):
        """Test that first series succeeds but the second fails, returns error"""
        key1 = "series_maddbulk_partial_ok"
        key2 = "series_maddbulk_partial_bad"
        payload1 = r'{"values":[1,2],"timestamps":[1000,2000]}'
        payload2 = r'{"values":[10],"timestamps":[1000,2000]}'  # length mismatch

        self.client.execute_command("TS.CREATE", key1)
        self.client.execute_command("TS.CREATE", key2)

        with pytest.raises(ResponseError):
            self.client.execute_command("TS.MADDBULK", key1, payload1, key2, payload2)

        # First series should have been created despite the second failing
        assert self.client.execute_command("EXISTS", key1) == 1

    def test_maddbulk_large_batch(self):
        """Test bulk ingestion with a large number of samples"""
        key = "series_maddbulk_large"
        n = 1000
        timestamps = list(range(n))
        values = list(range(n))

        payload = f'{{"values":{values},"timestamps":{timestamps}}}'
        self.client.execute_command("TS.CREATE", key)
        res = self.client.execute_command("TS.MADDBULK", key, payload)
        assert res == [[n, n]]

        # Verify count
        info = self.ts_info(key)
        assert info['totalSamples'] == n

    def test_maddbulk_unsorted_timestamps(self):
        """Test that unsorted timestamps are handled correctly"""
        key = "series_maddbulk_unsorted"
        payload = r'{"values":[3,1,2],"timestamps":[3000,1000,2000]}'

        self.client.execute_command("TS.CREATE", key)
        res = self.client.execute_command("TS.MADDBULK", key, payload)
        assert res == [[3, 3]]

        # Verify samples are stored in sorted order
        range_res = self.client.execute_command("TS.RANGE", key, "-", "+")
        assert range_res[0][0] == 1000
        assert range_res[1][0] == 2000
        assert range_res[2][0] == 3000

    def test_maddbulk_with_compaction_rules(self):
        """Test that MADDBULK triggers compaction rules"""
        src = "series_maddbulk_compact_src"
        dest = "series_maddbulk_compact_dest"

        self.client.execute_command("TS.CREATE", src)
        self.client.execute_command("TS.CREATE", dest)
        self.client.execute_command("TS.CREATERULE", src, dest, "AGGREGATION", "SUM", 10)

        # Ingest samples spanning two buckets: [0..9] and [10..19]
        payload = r'{"values":[1,2,3],"timestamps":[1,2,11]}'
        res = self.client.execute_command("TS.MADDBULK", src, payload)
        assert res == [[3, 3]]

        # Verify compaction occurred
        dest_range = self.client.execute_command("TS.RANGE", dest, "-", "+")
        assert len(dest_range) >= 1
        assert dest_range[0] == [0, b"3"]  # bucket 0: 1+2=3
