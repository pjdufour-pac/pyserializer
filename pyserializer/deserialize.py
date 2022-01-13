# =================================================================
#
# Work of the U.S. Department of Defense, Defense Digital Service.
# Released as open source under the MIT License.  See LICENSE file.
#
# =================================================================

import csv
import gzip
import io
import json
import sys
import zipfile

import pyarrow.parquet as pq

from pyserializer.cleaner import clean


def deserialize(
    src=None,
    format=None,
    compression=None,
    schema=None,
    filters=None,
    buffer_size=None,
    fs=None,
    drop_blanks=None,
    drop_nulls=None,
    name=None
):

    if format == "csv" or format == "tsv":
        if compression == "gzip":
            if src == "-":
                data = None
                with gzip.open(sys.stdin.buffer, mode='rb') as f:
                    data = [x for x in csv.DictReader(f, delimiter=("\t" if format == "tsv" else ","))]
                if drop_nulls or drop_blanks:
                    return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                return data
            else:
                if fs is not None:
                    data = None
                    with fs.open(src, 'rb') as f:
                        with gzip.GzipFile(fileobj=f) as gf:
                            data = [x for x in csv.DictReader(gf, delimiter=("\t" if format == "tsv" else ","))]
                    if drop_nulls or drop_blanks:
                        return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                    return data
                else:
                    data = None
                    with gzip.open(src, 'rt') as f:
                        data = [x for x in csv.DictReader(f, delimiter=("\t" if format == "tsv" else ","))]
                    if drop_nulls or drop_blanks:
                        return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                    return data
        elif compression == "zip":
            if src == "-":
                raise Exception("cannot unzip stdout")
            else:
                if fs is not None:
                    raise Exception("cannot unzip from filesystem")
                else:
                    data = None
                    with zipfile.ZipFile(src, 'r') as zf:
                        with zf.open(name, 'r') as f:
                            data = [x for x in csv.DictReader(
                                io.TextIOWrapper(f),
                                delimiter=("\t" if format == "tsv" else ",")
                            )]
                    if drop_nulls or drop_blanks:
                        return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                    return data
        else:
            if src == "-":
                data = [x for x in csv.DictReader(
                    io.TextIOWrapper(sys.stdin.buffer),
                    delimiter=("\t" if format == "tsv" else ",")
                )]
                if drop_nulls or drop_blanks:
                    return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                return data
            else:
                if fs is not None:
                    data = None
                    with fs.open(src, 'rb') as f:
                        data = [x for x in csv.DictReader(f, delimiter=("\t" if format == "tsv" else ","))]
                    if drop_nulls or drop_blanks:
                        return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                    return data
                else:
                    data = None
                    with open(src, 'rt') as f:
                        data = [x for x in csv.DictReader(f, delimiter=("\t" if format == "tsv" else ","))]
                    if drop_nulls or drop_blanks:
                        return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                    return data
    elif format == "json":
        if compression == "gzip":
            if src == "-":
                data = None
                with gzip.open(sys.stdin.buffer, mode='rb') as f:
                    data = json.load(f)
                if drop_nulls or drop_blanks:
                    return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                return data
            else:
                if fs is not None:
                    data = None
                    with fs.open(src, 'rb') as f:
                        with gzip.GzipFile(fileobj=f) as gf:
                            data = json.load(gf)
                    if drop_nulls or drop_blanks:
                        return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                    return data
                else:
                    data = None
                    with gzip.open(src, 'rb') as f:
                        data = json.load(f)
                    if drop_nulls or drop_blanks:
                        return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                    return data
        else:
            if src == "-":
                data = json.load(sys.stdin.buffer)
                if drop_nulls or drop_blanks:
                    return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                return data
            else:
                if fs is not None:
                    data = None
                    with fs.open(src, 'rb') as f:
                        data = json.load(f)
                    if drop_nulls or drop_blanks:
                        return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                    return data
                else:
                    data = None
                    with open(src, 'rb') as f:
                        data = json.load(f)
                    if drop_nulls or drop_blanks:
                        return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                    return data
    elif format == "jsonl":
        if compression == "gzip":
            if src == "-":
                data = []
                with gzip.open(sys.stdin.buffer, mode='rb') as f:
                    while (line := f.readline()):
                        data += [json.loads(line[0:len(line)-1])]
                if drop_nulls or drop_blanks:
                    return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                return data
            else:
                if fs is not None:
                    data = []
                    with fs.open(src, 'rb') as f:
                        with gzip.GzipFile(fileobj=f) as gf:
                            while (line := gf.readline()):
                                data += [json.loads(line[0:len(line)-1])]
                    if drop_nulls or drop_blanks:
                        return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                    return data
                else:
                    data = []
                    with gzip.open(src, 'rt') as f:
                        data += [json.loads(line) for line in f.readlines()]
                    if drop_nulls or drop_blanks:
                        return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                    return data
        else:
            if src == "-":
                data = []
                while (line := sys.stdin.buffer.readline()):
                    data += [json.loads(line[0:len(line)-1])]
                if drop_nulls or drop_blanks:
                    return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                return data
            else:
                if fs is not None:
                    data = []
                    with fs.open(src, 'rb') as f:
                        while (line := f.readline()):
                            data += [json.loads(line[0:len(line)-1])]
                    if drop_nulls or drop_blanks:
                        return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                    return data
                else:
                    data = []
                    with open(src, 'rb') as f:
                        while (line := f.readline()):
                            if line[len(line)-1] == '\n':
                                data += [json.loads(line[0:len(line)-1])]
                            else:
                                data += [json.loads(line)]
                    if drop_nulls or drop_blanks:
                        return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
                    return data
    elif format == "parquet":
        dataset = pq.ParquetDataset(
            src,
            schema=schema,
            filters=filters,
            validate_schema=False,
            buffer_size=buffer_size if buffer_size is not None else 4096,
            filesystem=fs,
        )
        data = dataset.read().to_pandas().to_dict('records')
        if drop_nulls or drop_blanks:
            return clean(data, drop_nulls=drop_nulls, drop_blanks=drop_blanks)
        return data
    else:
        raise Exception("invalid format "+format)
    return None
