# =================================================================
#
# Work of the U.S. Department of Defense, Defense Digital Service.
# Released as open source under the MIT License.  See LICENSE file.
#
# =================================================================

import datetime
import decimal
import json
from multiprocessing import get_context
import os
import shutil
import tempfile
import unittest

import pandas as pd

from pyserializer.deserialize import deserialize
from pyserializer.encoder import Encoder
from pyserializer.parquet import DatasetWriter
from pyserializer.serialize import serialize


class TestEncoder(unittest.TestCase):

    def test_decimal_default(self):
        self.assertEqual(
            json.dumps(decimal.Decimal('123.456'), cls=Encoder),
            '123.456',
            'error encoding decimal'
        )

    def test_decimal_float(self):
        self.assertEqual(
            json.dumps(decimal.Decimal('123.456'), cls=Encoder, formats={"decimal": "float"}),
            '123.456',
            'error encoding decimal as float'
        )

    def test_decimal_string(self):
        self.assertEqual(
            json.dumps(decimal.Decimal('123.456'), cls=Encoder, formats={"decimal": "string"}),
            '"123.456"',
            'error encoding decimal as string'
        )

    def test_exception(self):
        self.assertEqual(
            json.dumps(Exception("hello world"), cls=Encoder),
            '"hello world"',
            'error encoding exception as string'
        )

    def test_string(self):
        self.assertEqual(
            json.dumps("hello world", cls=Encoder),
            '"hello world"',
            'error encoding string'
        )

    def test_float(self):
        self.assertEqual(
            json.dumps(123.456, cls=Encoder),
            '123.456',
            'error encoding float'
        )

    def test_date(self):
        self.assertEqual(
            json.dumps(datetime.date(2001, 1, 1), cls=Encoder),
            '"2001-01-01"',
            'error encoding date'
        )

    def test_date_custom(self):
        self.assertEqual(
            json.dumps(
                datetime.date(2001, 1, 1),
                cls=Encoder,
                formats={"date": "%m/%d/%Y"}
            ),
            '"01/01/2001"',
            'error encoding date with custom format'
        )

    def test_datetime(self):
        self.assertEqual(
            json.dumps(
                datetime.datetime(2001, 1, 1, hour=1, minute=1, second=1),
                cls=Encoder
            ),
            '"2001-01-01T01:01:01.000000"',
            'error encoding datetime'
        )

    def test_datetime_custom(self):
        self.assertEqual(
            json.dumps(
                datetime.datetime(2001, 1, 1, hour=1, minute=1, second=1),
                cls=Encoder,
                formats={"datetime": "%m/%d/%Y %H:%M:%S"}
            ),
            '"01/01/2001 01:01:01"',
            'error encoding datetime using custom format'
        )

    def test_timestamp(self):
        self.assertEqual(
            json.dumps(
                pd.Timestamp(year=2001, month=1, day=1, hour=1, minute=1, second=1, microsecond=1, nanosecond=0),
                cls=Encoder
            ),
            '"2001-01-01T01:01:01.000001"',
            'error encoding pandas timestamp'
        )

    def test_timestamp_custom(self):
        self.assertEqual(
            json.dumps(
                pd.Timestamp(year=2001, month=1, day=1, hour=1, minute=1, second=1, microsecond=1, nanosecond=0),
                cls=Encoder,
                formats={"timestamp": "%m/%d/%Y %H:%M:%S.%f"}
            ),
            '"01/01/2001 01:01:01.000001"',
            'error encoding custom pandas timestamp'
        )

    def test_dataframe(self):
        df = pd.DataFrame([{"hello": "world"}, {"hello": "planet"}])
        self.assertEqual(
            json.dumps(df, cls=Encoder),
            '[{"hello": "world"}, {"hello": "planet"}]',
            'error encoding decimal'
        )


class TestSerializer(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_serialize_csv(self):
        #
        test_dir = os.path.join(self.test_dir, 'test_serialize_csv')
        os.makedirs(test_dir, exist_ok=True)
        test_file = os.path.join(test_dir, 'data.csv')
        #
        data = [
            {"hello": "world", "ciao": "sun", "order": "1"},
            {"hello": "world", "ciao": "moon", "order": "2"},
            {"hello": "planet", "ciao": "sun", "order": "3"},
            {"hello": "planet", "ciao": "moon", "order": "4"}
        ]
        #
        serialize(
            dest=test_file,
            data=data,
            format="csv",
        )
        #
        result = deserialize(src=test_file, format="csv")
        #
        self.assertEqual(
            sorted(result, key=lambda x: x['order']),
            sorted(data, key=lambda x: x['order']),
            'error serializing to csv and then deserializing back'
        )

    def test_serialize_json_decimal_default(self):
        test_dir = os.path.join(self.test_dir, 'test_serialize_json_decimal_default')
        os.makedirs(test_dir, exist_ok=True)
        test_file = os.path.join(test_dir, 'data.json')
        data = decimal.Decimal('123.456')
        serialize(dest=test_file, data=data, format="json")
        result = None
        with open(test_file, mode='rt') as f:
            result = f.read()
        self.assertEqual(
            result,
            '123.456',
            'error encoding decimal'
        )

    def test_serialize_json_string_default(self):
        test_dir = os.path.join(self.test_dir, 'test_serialize_json_string_default')
        os.makedirs(test_dir, exist_ok=True)
        test_file = os.path.join(test_dir, 'data.json')
        data = "hello world"
        serialize(dest=test_file, data=data, format="json")
        result = None
        with open(test_file, mode='rt') as f:
            result = f.read()
        self.assertEqual(
            result,
            '"hello world"',
            'error encoding string as json'
        )

    def test_serialize_json_dataframe_default(self):
        test_dir = os.path.join(self.test_dir, 'test_serialize_json_dataframe_default')
        os.makedirs(test_dir, exist_ok=True)
        test_file = os.path.join(test_dir, 'data.json')
        data = pd.DataFrame([{"hello": "world"}, {"hello": "planet"}])
        serialize(dest=test_file, data=data, format="json")
        result = None
        with open(test_file, mode='rt') as f:
            result = f.read()
        self.assertEqual(
            result,
            '[{"hello":"world"},{"hello":"planet"}]',
            'error encoding data frame as json'
        )

    def test_serialize_json_dict_default(self):
        test_dir = os.path.join(self.test_dir, 'test_serialize_json_dict_default')
        os.makedirs(test_dir, exist_ok=True)
        test_file = os.path.join(test_dir, 'data.json')
        data = {"hello": "world"}
        serialize(dest=test_file, data=data, format="json")
        result = None
        with open(test_file, mode='rt') as f:
            result = f.read()
        self.assertEqual(
            result,
            '{"hello":"world"}',
            'error encoding dict as json'
        )

    def test_serialize_jsonl_dataframe_default(self):
        test_dir = os.path.join(self.test_dir, 'test_serialize_json_dataframe_default')
        os.makedirs(test_dir, exist_ok=True)
        test_file = os.path.join(test_dir, 'data.json')
        data = pd.DataFrame([{"hello": "world"}, {"hello": "planet"}])
        serialize(dest=test_file, data=data, format="jsonl")
        result = None
        with open(test_file, mode='rt') as f:
            result = f.read()
        self.assertEqual(
            result,
            '{"hello":"world"}\n{"hello":"planet"}',
            'error encoding data frame as JSON Lines (jsonl)'
        )

    def test_serialize_jsonl_dataframe_index(self):
        test_dir = os.path.join(self.test_dir, 'test_serialize_jsonl_dataframe_index')
        os.makedirs(test_dir, exist_ok=True)
        test_file = os.path.join(test_dir, 'data.json')
        data = pd.DataFrame([{"hello": "world"}, {"hello": "planet"}])
        serialize(dest=test_file, data=data, format="jsonl", index=True)
        result = None
        with open(test_file, mode='rt') as f:
            result = f.read()
        self.assertEqual(
            result,
            '{"Index":0,"hello":"world"}\n{"Index":1,"hello":"planet"}',
            'error encoding data frame as JSON lines (jsonl) with index'
        )

    def test_serialize_jsonl_dataframe_pretty(self):
        test_dir = os.path.join(self.test_dir, 'test_serialize_jsonl_dataframe_pretty')
        os.makedirs(test_dir, exist_ok=True)
        test_file = os.path.join(test_dir, 'data.json')
        data = pd.DataFrame([{"hello": "world"}, {"hello": "planet"}])
        serialize(dest=test_file, data=data, format="jsonl", pretty=True)
        result = None
        with open(test_file, mode='rt') as f:
            result = f.read()
        self.assertEqual(
            result,
            '{"hello": "world"}\n{"hello": "planet"}',
            'error encoding data frame as JSON Lines (jsonl) with pretty spaces'
        )

    def test_parquet_format_path(self):
        test_dir = os.path.join(self.test_dir, 'test_parquet_dataset')
        os.makedirs(test_dir, exist_ok=True)
        test_dir_dataset = os.path.join(test_dir, 'data')
        #
        dt = datetime.datetime(2001, 1, 1, hour=1, minute=1, second=1)
        #
        partition_columns = ["year", "month", "day", "hour", "minute"]
        partition_values = [dt.year, dt.month, dt.day, dt.hour, dt.minute]
        #
        dw = DatasetWriter(
            test_dir_dataset,
            partition_columns,
            compression=None,
            filesystem=None,
            makedirs=True,
            nthreads=1,
            preserve_index=False,
            schema=None,
        )
        #
        self.assertEqual(
            dw.format_partition_parent(partition_values),
            "year={}/month={}/day={}/hour={}/minute={}".format(*partition_values),
            'error formatting path to parquet partition directory'
        )
        #
        self.assertEqual(
            dw.format_partition_filename(partition_values),
            "{}-{}-{}-{}-{}.parquet".format(*partition_values),
            'error formatting path to parquet partition filename'
        )

    def test_roundtrip_parquet_partition(self):
        test_dir = os.path.join(self.test_dir, 'test_roundtrip_parquet_partition')
        os.makedirs(test_dir, exist_ok=True)
        test_file_parquet = os.path.join(test_dir, 'test.parquet')
        #
        data = [
            {"hello": "world", "ciao": "sun", "order": 1},
            {"hello": "world", "ciao": "moon", "order": 2},
            {"hello": "planet", "ciao": "sun", "order": 3},
            {"hello": "planet", "ciao": "moon", "order": 4}
        ]
        #
        serialize(dest=test_file_parquet, data=data, format="parquet")
        #
        result = deserialize(src=test_file_parquet, format="parquet")
        #
        self.assertEqual(
            sorted(result, key=lambda x: x['order']),
            sorted(data, key=lambda x: x['order']),
            'error serializing to parquet and then deserializing back'
        )

    def test_roundtrip_parquet_dataset(self):
        #
        ctx = get_context("spawn")
        test_dir = os.path.join(self.test_dir, 'test_roundtrip_parquet_dataset')
        os.makedirs(test_dir, exist_ok=True)
        test_dir_dataset = os.path.join(test_dir, 'data')
        #
        data = [
            {"hello": "world", "ciao": "sun", "order": 1},
            {"hello": "world", "ciao": "moon", "order": 2},
            {"hello": "planet", "ciao": "sun", "order": 3},
            {"hello": "planet", "ciao": "moon", "order": 4}
        ]
        #
        serialize(
            ctx=ctx,
            dest=test_dir_dataset,
            data=data,
            format="parquet",
            partition_columns=["hello"],
            makedirs=True,
            row_group_columns=["ciao"],
            zero_copy_only=True,
        )
        #
        result = deserialize(src=test_dir_dataset, format="parquet")
        #
        self.assertEqual(
            sorted(result, key=lambda x: x['order']),
            sorted(data, key=lambda x: x['order']),
            'error serializing to parquet and then deserializing back'
        )

    def test_roundtrip_csv_gzip(self):
        #
        test_dir = os.path.join(self.test_dir, 'test_roundtrip_csv_gzip')
        os.makedirs(test_dir, exist_ok=True)
        test_file = os.path.join(test_dir, 'data.csv.gz')
        #
        data = [
            {"hello": "world", "ciao": "sun", "order": "1"},
            {"hello": "world", "ciao": "moon", "order": "2"},
            {"hello": "planet", "ciao": "sun", "order": "3"},
            {"hello": "planet", "ciao": "moon", "order": "4"}
        ]
        #
        serialize(
            compression="gzip",
            dest=test_file,
            data=data,
            format="csv",
        )
        #
        result = deserialize(
            compression="gzip",
            format="csv",
            src=test_file,
        )
        #
        self.assertEqual(
            sorted(result, key=lambda x: x['order']),
            sorted(data, key=lambda x: x['order']),
            'error serializing to csv and then deserializing back'
        )

    def test_roundtrip_json_gzip(self):
        #
        test_dir = os.path.join(self.test_dir, 'test_roundtrip_json_gzip')
        os.makedirs(test_dir, exist_ok=True)
        test_file = os.path.join(test_dir, 'data.json.gz')
        #
        data = [
            {"hello": "world", "ciao": "sun", "order": "1"},
            {"hello": "world", "ciao": "moon", "order": "2"},
            {"hello": "planet", "ciao": "sun", "order": "3"},
            {"hello": "planet", "ciao": "moon", "order": "4"}
        ]
        #
        serialize(
            compression="gzip",
            dest=test_file,
            data=data,
            format="json",
        )
        #
        result = deserialize(
            compression="gzip",
            format="json",
            src=test_file,
        )
        #
        self.assertEqual(
            sorted(result, key=lambda x: x['order']),
            sorted(data, key=lambda x: x['order']),
            'error serializing to json and then deserializing back'
        )

    def test_roundtrip_jsonl_gzip(self):
        #
        test_dir = os.path.join(self.test_dir, 'test_roundtrip_jsonl_gzip')
        os.makedirs(test_dir, exist_ok=True)
        test_file = os.path.join(test_dir, 'data.jsonl.gz')
        #
        data = [
            {"hello": "world", "ciao": "sun", "order": "1"},
            {"hello": "world", "ciao": "moon", "order": "2"},
            {"hello": "planet", "ciao": "sun", "order": "3"},
            {"hello": "planet", "ciao": "moon", "order": "4"}
        ]
        #
        serialize(
            compression="gzip",
            dest=test_file,
            data=data,
            format="jsonl",
        )
        #
        result = deserialize(
            compression="gzip",
            format="jsonl",
            src=test_file,
        )
        #
        self.assertEqual(
            sorted(result, key=lambda x: x['order']),
            sorted(data, key=lambda x: x['order']),
            'error serializing to json lines (jsonl ) and then deserializing back'
        )
