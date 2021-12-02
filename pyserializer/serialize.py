# =================================================================
#
# Work of the U.S. Department of Defense, Defense Digital Service.
# Released as open source under the MIT License.  See LICENSE file.
#
# =================================================================

import json
import gzip
import csv
import sys

import pyarrow as pa
import pandas as pd

from pyserializer.encoder import Encoder
from pyserializer.parquet import DatasetWriter, PartitionWriter


def serialize(
    ctx=None,
    dest=None,
    data=None,
    encoder=None,
    format=None,
    compression=None,
    columns=None,
    limit=None,
    partition_columns=None,
    row_group_columns=None,
    row_group_size=None,
    fs=None,
    schema=None,
    makedirs=False,
    index=False,
    safe=True,
    zero_copy_only=False,
    pretty=False
):
    if format == "json":

        kwargs = {
            "cls": (encoder if encoder is not None else Encoder),
            "separators": ((', ', ': ') if pretty else (',', ':'))
        }

        if compression == "gzip":
            if dest == "-":
                json.dump(data, sys.stdout, **kwargs)
            else:
                if fs is not None:
                    with fs.open(dest, 'wb') as f:
                        with gzip.open(f, 'wt') as w:
                            w.write(json.dumps(data, **kwargs))
                else:
                    with gzip.open(dest, 'wt') as f:
                        json.dump(data, f, **kwargs)
        else:
            if dest == "-":
                print(json.dumps(data, **kwargs))
            else:
                if fs is not None:
                    with fs.open(dest, 'wt') as f:
                        f.write(json.dumps(data, **kwargs))
                else:
                    with open(dest, 'wt') as f:
                        json.dump(data, f, **kwargs)

    elif format == "jsonl":

        if (not isinstance(data, pa.Table)) and (not isinstance(data, pd.DataFrame)) and (not isinstance(data, list)):
            raise Exception("unknown data type {}".format(type(data)))

        if len(data) == 0:
            return

        kwargs = {
            "cls": (encoder if encoder is not None else Encoder),
            "separators": ((', ', ': ') if pretty else (',', ':'))
        }

        # if list, then slice the list all at once, since already in memory
        if isinstance(data, list):
            if compression == "gzip":
                if dest == "-":
                    json.dump(data[0], sys.stdout, **kwargs)
                    for item in (data[1:limit] if limit is not None and limit > 0 else data[1:]):
                        f.write("\n")
                        json.dump(item, sys.stdout, **kwargs)
                else:
                    with gzip.open(dest, 'wt') as f:
                        json.dump(data[0], f, **kwargs)
                        for item in (data[1:limit] if limit is not None and limit > 0 else data[1:]):
                            f.write("\n")
                            json.dump(item, f, **kwargs)
            else:
                if dest == "-":
                    print(json.dumps(data[0], **kwargs))
                    for item in (data[1:limit] if limit is not None and limit > 0 else data[1:]):
                        print("")
                        print(json.dumps(item, **kwargs))
                else:
                    with open(dest, 'wt') as f:
                        json.dump(data[0], f, **kwargs)
                        for item in (data[1:limit] if limit is not None and limit > 0 else data[1:]):
                            f.write("\n")
                            json.dump(item, f, **kwargs)

        # if dataframe, then iterate through the data time.
        if isinstance(data, pd.DataFrame):
            if compression == "gzip":
                if dest == "-":
                    if limit is not None and limit > 0 and limit < len(data):
                        with gzip.open(sys.stdout, 'wb') as f:
                            count = 0
                            tuples = data.itertuples(index=index)
                            json.dump(next(tuples)._asdict(), f, **kwargs)
                            for item in tuples:
                                f.write("\n")
                                json.dump(item._asdict(), f, **kwargs)
                                count += 1
                                if count >= limit:
                                    break
                    else:
                        with gzip.open(sys.stdout, 'wb') as f:
                            tuples = data.itertuples(index=index)
                            json.dump(next(tuples)._asdict(), f, **kwargs)
                            for item in tuples:
                                f.write("\n")
                                json.dump(item._asdict(), f, **kwargs)
                else:
                    if limit is not None and limit > 0 and limit < len(data):
                        with gzip.open(dest, 'wb') as f:
                            count = 0
                            tuples = data.itertuples(index=index)
                            json.dump(next(tuples)._asdict(), f, **kwargs)
                            for item in tuples:
                                f.write("\n")
                                json.dump(item._asdict(), f, **kwargs)
                                count += 1
                                if count >= limit:
                                    break
                    else:
                        with gzip.open(dest, 'wb') as f:
                            tuples = data.itertuples(index=index)
                            json.dump(next(tuples)._asdict(), f, **kwargs)
                            for item in tuples:
                                f.write("\n")
                                json.dump(item._asdict(), f, **kwargs)
            else:
                if dest == "-":
                    if limit is not None and limit > 0 and limit < len(data):
                        count = 0
                        tuples = data.itertuples(index=index)
                        print(json.dumps(next(tuples)._asdict(), **kwargs))
                        for item in tuples:
                            print("")
                            print(json.dumps(item._asdict(), **kwargs))
                            count += 1
                            if count >= limit:
                                break
                    else:
                        tuples = data.itertuples(index=index)
                        print(json.dumps(next(tuples)._asdict(), **kwargs))
                        for item in tuples:
                            print("")
                            print(json.dumps(item._asdict(), **kwargs))
                else:
                    if limit is not None and limit > 0 and limit < len(data):
                        with open(dest, 'wb') as f:
                            count = 0
                            tuples = data.itertuples(index=index)
                            json.dump(next(tuples)._asdict(), f, **kwargs)
                            for item in tuples:
                                f.write("\n")
                                json.dump(item._asdict(), f, **kwargs)
                                count += 1
                                if count >= limit:
                                    break
                    else:
                        with open(dest, 'wt') as f:
                            tuples = data.itertuples(index=index)
                            json.dump(next(tuples)._asdict(), f, **kwargs)
                            for item in tuples:
                                f.write("\n")
                                json.dump(item._asdict(), f, **kwargs)

    elif format == "csv":
        fieldnames = columns or sorted(list({k for d in data for k in d.keys()}))
        if compression == "gzip":
            if dest == "-":
                if limit is not None and limit > 0 and limit < len(data):
                    w = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
                    w.writeheader()
                    count = 0
                    for r in data:
                        w.writerow(r)
                        count += 1
                        if count >= limit:
                            break
                else:
                    w = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
                    w.writeheader()
                    for r in data:
                        w.writerow(r)
            else:
                if limit is not None and limit > 0 and limit < len(data):
                    with gzip.open(dest, 'wt') as f:
                        w = csv.DictWriter(f, fieldnames=fieldnames)
                        w.writeheader()
                        count = 0
                        for r in data:
                            w.writerow(r)
                            count += 1
                            if count >= limit:
                                break
                else:
                    with gzip.open(dest, 'wt') as f:
                        w = csv.DictWriter(f, fieldnames=fieldnames)
                        w.writeheader()
                        for r in data:
                            w.writerow(r)
        else:
            if dest == "-":
                if limit is not None and limit > 0 and limit < len(data):
                    w = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
                    w.writeheader()
                    count = 0
                    for r in data:
                        w.writerow(r)
                        count += 1
                        if count >= limit:
                            break
                else:
                    w = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
                    w.writeheader()
                    for r in data:
                        w.writerow(r)
            else:
                if limit is not None and limit > 0 and limit < len(data):
                    with open(dest, 'wt') as f:
                        w = csv.DictWriter(f, fieldnames=fieldnames)
                        w.writeheader()
                        count = 0
                        for r in data:
                            w.writerow(r)
                            count += 1
                            if count >= limit:
                                break
                else:
                    with open(dest, 'wt') as f:
                        w = csv.DictWriter(f, fieldnames=fieldnames)
                        w.writeheader()
                        for r in data:
                            w.writerow(r)


    elif format == "parquet":

        if (not isinstance(data, pa.Table)) and (not isinstance(data, pd.DataFrame)) and (not isinstance(data, list)):
            raise Exception("unknown dataset type")

        if len(data) == 0:
            return

        if partition_columns is not None and len(partition_columns) > 0:
            dw = DatasetWriter(
                dest,
                partition_columns,
                compression=compression.upper() if compression in ['gzip', 'snappy'] else None,
                filesystem=fs,
                makedirs=makedirs,
                nthreads=None,
                preserve_index=index,
                schema=schema
            )
            dw.write_dataset(
                data,
                ctx=ctx,
                row_group_columns=row_group_columns,
                row_group_size=row_group_size,
                safe=True,
                limit=limit
            )
        else:
            table = None
            if isinstance(data, pd.DataFrame):
                table = pa.Table.from_pandas(data, preserve_index=index)
            elif isinstance(data, pa.Table):
                table = data
            elif isinstance(data, list):
                table = pa.Table.from_pandas(pd.DataFrame(data), preserve_index=index)
            pw = PartitionWriter(
                dest,
                table.schema,
                compression=compression.upper() if compression in ['gzip', 'snappy'] else None,
                filesystem=fs)
            pw.write_partition(
                table,
                row_group_size=row_group_size,
                row_group_columns=row_group_columns,
                preserve_index=index,
                safe=safe,
                limit=limit)
            pw.close()
    else:
        raise Exception("invalid format {}".format(format))
