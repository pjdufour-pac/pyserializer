# =================================================================
#
# Work of the U.S. Department of Defense, Defense Digital Service.
# Released as open source under the MIT License.  See LICENSE file.
#
# =================================================================

import csv
import json

import pyarrow as pa
import pandas as pd

from pyserializer.cleaner import clean
from pyserializer.encoder import Encoder
from pyserializer.parquet import DatasetWriter, PartitionWriter
from pyserializer.writer import create_writer


def write_jsonl_tuples(drop_blanks=None, drop_nulls=None, f=None, limit=None, tuples=None, kwargs=None):
    if limit is not None and limit > 0 and limit < len(tuples):
        if drop_nulls or drop_blanks:
            count = 0
            for item in tuples:
                f.write(
                    json.dumps(
                        clean(
                            item._asdict(),
                            drop_nulls=drop_nulls,
                            drop_blanks=drop_blanks
                        ),
                        **kwargs
                    )+"\n"
                )
                count += 1
                if count >= limit:
                    break
        else:
            count = 0
            for item in tuples:
                f.write(json.dumps(item._asdict(), **kwargs)+"\n")
                count += 1
                if count >= limit:
                    break
    else:
        if drop_nulls or drop_blanks:
            for item in tuples:
                f.write(
                    json.dumps(
                        clean(
                            item._asdict(),
                            drop_nulls=drop_nulls,
                            drop_blanks=drop_blanks
                        ),
                        **kwargs
                    )+"\n"
                )
        else:
            for item in tuples:
                f.write(json.dumps(item._asdict(), **kwargs)+"\n")


def write_csv_tuples(drop_blanks=None, drop_nulls=None, cw=None, limit=None, tuples=None):
    if limit is not None and limit > 0 and limit < len(tuples):
        if drop_nulls or drop_blanks:
            count = 0
            for item in tuples:
                cw.writerow(clean(
                    item._asdict(),
                    drop_nulls=drop_nulls,
                    drop_blanks=drop_blanks
                ))
                count += 1
                if count >= limit:
                    break
        else:
            count = 0
            for item in tuples:
                cw.writerow(item._asdict())
                count += 1
                if count >= limit:
                    break
    else:
        if drop_nulls or drop_blanks:
            for item in tuples:
                cw.writerow(clean(
                    item._asdict(),
                    drop_nulls=drop_nulls,
                    drop_blanks=drop_blanks
                ))
        else:
            for item in tuples:
                cw.writerow(item._asdict())


def serialize(
    allow_nan=False,
    ctx=None,
    dest=None,
    data=None,
    drop_blanks=None,
    drop_nulls=None,
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
    timeout=None,
    zero_copy_only=False,
    pretty=False
):
    if format == "json":

        kwargs = {
            "allow_nan": allow_nan,
            "cls": (encoder if encoder is not None else Encoder),
            "separators": ((', ', ': ') if pretty else (',', ':'))
        }

        if fs is not None:
            with fs.open(dest, 'wb') as f:
                with create_writer(f=f, compression=compression) as w:
                    w.write(json.dumps(data, **kwargs))
        else:
            with create_writer(f=dest, compression=compression) as w:
                w.write(json.dumps(data, **kwargs))

    elif format == "jsonl":

        if (not isinstance(data, pa.Table)) and (not isinstance(data, pd.DataFrame)) and (not isinstance(data, list)):
            raise Exception("unknown data type {}".format(type(data)))

        if len(data) == 0:
            return

        kwargs = {
            "allow_nan": allow_nan,
            "cls": (encoder if encoder is not None else Encoder),
            "separators": ((', ', ': ') if pretty else (',', ':'))
        }

        if fs is not None:
            with fs.open(dest, 'wb') as f:
                with create_writer(f=f, compression=compression) as w:

                    # if list, then slice the list all at once, since already in memory
                    if isinstance(data, list):
                        json.dump(data[0], w, **kwargs)
                        for item in (data[1:limit] if limit is not None and limit > 0 else data[1:]):
                            w.write("\n")
                            json.dump(item, w, **kwargs)

                    # if dataframe, then iterate through the data time.
                    if isinstance(data, pd.DataFrame):
                        write_jsonl_tuples(
                            drop_blanks=drop_blanks,
                            drop_nulls=drop_nulls,
                            f=w,
                            limit=limit,
                            tuples=data.itertuples(index=index),
                            kwargs=kwargs
                        )

        else:
            with create_writer(f=dest, compression=compression) as w:

                # if list, then slice the list all at once, since already in memory
                if isinstance(data, list):
                    json.dump(data[0], w, **kwargs)
                    for item in (data[1:limit] if limit is not None and limit > 0 else data[1:]):
                        w.write("\n")
                        json.dump(item, w, **kwargs)

                # if dataframe, then iterate through the data time.
                if isinstance(data, pd.DataFrame):
                    write_jsonl_tuples(
                        drop_blanks=drop_blanks,
                        drop_nulls=drop_nulls,
                        f=w,
                        limit=limit,
                        tuples=data.itertuples(index=index),
                        kwargs=kwargs
                    )

    elif format == "csv":

        if (not isinstance(data, pa.Table)) and (not isinstance(data, pd.DataFrame)) and (not isinstance(data, list)):
            raise Exception("unknown data type {}".format(type(data)))

        if len(data) == 0:
            return

        if fs is not None:
            with fs.open(dest, 'wb') as f:
                with create_writer(f=f, compression=compression) as w:

                    # if list, then slice the list all at once, since already in memory
                    if isinstance(data, list):
                        fieldnames = columns or sorted(list({k for d in data for k in d.keys()}))
                        if limit is not None and limit > 0 and limit < len(data):
                            cw = csv.DictWriter(w, fieldnames=fieldnames)
                            cw.writeheader()
                            count = 0
                            for r in data:
                                cw.writerow(r)
                                count += 1
                                if count >= limit:
                                    break
                        else:
                            cw = csv.DictWriter(w, fieldnames=fieldnames)
                            cw.writeheader()
                            for r in data:
                                cw.writerow(r)

                    # if dataframe, then iterate through the data time.
                    if isinstance(data, pd.DataFrame):
                        fieldnames = sorted(list(data.columns))
                        cw = csv.DictWriter(w, fieldnames=fieldnames)
                        cw.writeheader()
                        if limit is not None and limit > 0 and limit < len(data):
                            data = data.head(limit)
                        else:
                            write_csv_tuples(
                                drop_blanks=drop_blanks,
                                drop_nulls=drop_nulls,
                                cw=cw,
                                limit=limit,
                                tuples=data.itertuples(index=index)
                            )

        else:
            with create_writer(f=dest, compression=compression) as w:

                # if list, then slice the list all at once, since already in memory
                if isinstance(data, list):
                    fieldnames = columns or sorted(list({k for d in data for k in d.keys()}))
                    if limit is not None and limit > 0 and limit < len(data):
                        cw = csv.DictWriter(w, fieldnames=fieldnames)
                        cw.writeheader()
                        count = 0
                        for r in data:
                            cw.writerow(r)
                            count += 1
                            if count >= limit:
                                break
                    else:
                        cw = csv.DictWriter(w, fieldnames=fieldnames)
                        cw.writeheader()
                        for r in data:
                            cw.writerow(r)

                # if dataframe, then iterate through the data time.
                if isinstance(data, pd.DataFrame):
                    fieldnames = sorted(list(data.columns))
                    cw = csv.DictWriter(w, fieldnames=fieldnames)
                    cw.writeheader()
                    if limit is not None and limit > 0 and limit < len(data):
                        data = data.head(limit)
                    else:
                        write_csv_tuples(
                            drop_blanks=drop_blanks,
                            drop_nulls=drop_nulls,
                            cw=cw,
                            limit=limit,
                            tuples=data.itertuples(index=index)
                        )

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
                schema=schema,
                timeout=timeout
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
