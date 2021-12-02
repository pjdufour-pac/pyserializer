# =================================================================
#
# Work of the U.S. Department of Defense, Defense Digital Service.
# Released as open source under the MIT License.  See LICENSE file.
#
# =================================================================

import os

import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd


class PartitionWriter():

    def __init__(self, where, schema, compression=None, filesystem=None, zero_copy_only=False):
        self.where = where
        self.schema = schema
        self.compression = compression
        self.filesystem = filesystem
        self.writer = pq.ParquetWriter(
            self.where,
            self.schema,
            compression=self.compression,
            filesystem=self.filesystem)
        self.zero_copy_only = zero_copy_only

    def write_partition(
        self,
        data,
        row_group_size=None,
        row_group_columns=None,
        preserve_index=False,
        safe=True,
        limit=None
    ):
        if limit is not None and limit == 0:
            return
        if row_group_columns is not None and len(row_group_columns) > 0:
            df = None
            if isinstance(data, pd.DataFrame):
                df = data
            elif isinstance(data, pa.Table):
                df = data.to_pandas(zero_copy_only=self.zero_copy_only)
            elif isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                raise Exception("error writing parquet partition: unknown data type {}".format(type(data)))
            if limit is not None and limit > 0:
                df = df.head(limit)
            # create row groups for each column
            for keys, rg in df.groupby([data[column] for column in row_group_columns]):
                # write each row group
                self.writer.write_table(
                    pa.Table.from_pandas(
                        rg,
                        schema=self.schema,
                        preserve_index=preserve_index,
                        safe=safe
                    ),
                    row_group_size=row_group_size
                )
        else:
            table = None
            if isinstance(data, pd.DataFrame):
                table = pa.Table.from_pandas(
                    data.head(limit) if limit is not None and limit > 0 else data,
                    schema=self.schema,
                    preserve_index=preserve_index,
                    safe=safe
                )
            elif isinstance(data, pa.Table):
                if limit is not None and limit > 0 and limit < len(data):
                    table = pa.Table.from_pandas(
                        data.to_pandas(zero_copy_only=self.zero_copy_only).head(limit),
                        schema=self.schema,
                        preserve_index=preserve_index,
                        safe=safe
                    )
                else:
                    table = data
            elif isinstance(data, list):
                table = pa.Table.from_pandas(
                    pd.DataFrame(data[0:limit] if limit is not None and limit > 0 and limit < len(data) else data),
                    schema=self.schema,
                    preserve_index=preserve_index,
                    safe=safe
                )
            else:
                raise Exception("error writing parquet partition: unknown data type {}".format(type(data)))
            # write entire data frame as 1 row group
            self.writer.write_table(table, row_group_size=row_group_size)

    def close(self):
        self.writer.close()


def write_partition(
    where,
    schema,
    df,
    compression=None,
    filesystem=None,
    row_group_size=None,
    row_group_columns=None,
    preserve_index=False,
    safe=True,
    zero_copy_only=None
):

    pw = PartitionWriter(
        where,
        schema,
        compression=compression,
        filesystem=filesystem,
        zero_copy_only=zero_copy_only)

    pw.write_partition(
        df,
        row_group_size=row_group_size,
        row_group_columns=row_group_columns,
        preserve_index=preserve_index,
        safe=safe)

    pw.close()


class DatasetWriter():

    def __init__(
        self,
        where,
        partition_columns,
        compression=None,
        filesystem=None,
        makedirs=True,
        nthreads=None,
        preserve_index=None,
        schema=None,
        timeout=None,
        zero_copy_only=False
    ):

        self.where = where
        self.partition_columns = partition_columns
        self.compression = compression
        self.filesystem = filesystem
        self.preserve_index = preserve_index
        self.nthreads = nthreads if (nthreads is not None) and (nthreads > 0) else int(os.cpu_count()/2)
        self.makedirs = makedirs
        self.schema = schema
        self.timeout = timeout if timeout is not None else 600
        self.zero_copy_only = zero_copy_only

        if schema is not None:
            if not isinstance(schema, pa.Schema):
                raise Exception("schema is type {}, but expecting", type(schema), pa.Schema)

    def format_partition_value(self, value):
        if isinstance(value, bool):
            if value:
                return "1"
            else:
                return "0"
        elif isinstance(value, str):
            return value
        return str(value)

    def format_partition_directory(self, key, value):
        return '{}={}'.format(key, self.format_partition_value(value))

    def format_partition_filename(self, values):
        return '-'.join([self.format_partition_value(value) for value in values])+'.parquet'

    def format_partition_parent(self, values):
        return '/'.join([self.format_partition_directory(k, v) for k, v in zip(self.partition_columns, values)])

    def write_dataset(self, dataset, ctx=None, row_group_columns=None, row_group_size=None, safe=True, limit=None):

        if limit is not None and limit == 0:
            return

        if self.nthreads > 1:
            if ctx is None:
                raise Exception("ctx is not defined, but required when using {} threads".format(self.nthreads))

        df = None
        table = None

        if isinstance(dataset, pd.DataFrame):
            df = dataset
            table = pa.Table.from_pandas(
                df,
                schema=self.schema,
                preserve_index=self.preserve_index
            )
            if limit is not None and limit > 0:
                df = df.head(limit)
        elif isinstance(dataset, pa.Table):
            df = dataset.to_pandas(zero_copy_only=self.zero_copy_only)
            table = dataset
            if limit is not None and limit > 0:
                df = df.head(limit)
        elif isinstance(dataset, list):
            if limit is not None and limit > 0 and limit < len(dataset):
                dataset = dataset[0:limit]
            df = pd.DataFrame(dataset)
            table = pa.Table.from_pandas(
                df,
                schema=self.schema,
                preserve_index=self.preserve_index
            )

        if len(df.columns.drop(self.partition_columns)) == 0:
            raise ValueError('error writing parquet dataset: no data left to save outside partition columns')

        schema = table.schema

        for col in table.schema.names:
            if col in self.partition_columns:
                schema = schema.remove(schema.get_field_index(col))

        pool = ctx.Pool(processes=self.nthreads)

        results = []

        groups = df.drop(
            self.partition_columns,
            axis='columns').groupby([df[col] for col in self.partition_columns])

        for values, dfp in groups:

            if not isinstance(values, tuple):
                values = (values,)

            partition_directory = self.format_partition_parent(values)

            if self.makedirs:
                os.makedirs(os.path.join(self.where, partition_directory), exist_ok=True)

            where = os.path.join(
                self.where,
                partition_directory,
                self.format_partition_filename(values)
            )
            result = pool.apply_async(write_partition, args=(where, schema, dfp), kwds=dict(
                compression=self.compression,
                filesystem=self.filesystem,
                row_group_columns=row_group_columns,
                row_group_size=row_group_size,
                preserve_index=self.preserve_index,
                safe=safe,
                zero_copy_only=self.zero_copy_only,
            ))
            results += [result]

        pool.close()

        # Wait for all partitions to be written
        for i in range(len(results)):
            try:
                results[i].get(timeout=self.timeout)
            except Exception as err:
                print("error serializing partition", i, err)
                raise err
