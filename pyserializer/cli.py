# =================================================================
#
# Work of the U.S. Department of Defense, Defense Digital Service.
# Released as open source under the MIT License.  See LICENSE file.
#
# =================================================================

import fire
import os
from urllib.parse import urlparse

import s3fs
import pyathena
from pyathena.pandas.cursor import PandasCursor

from pyserializer.archive import names
from pyserializer.serialize import serialize
from pyserializer.deserialize import deserialize

algorithms = [
    "gzip",
    "zip"
]

formats = [
    "csv",
    "json",
    "jsonl",
    "parquet",
    "tsv"
]


def create_s3_filesystem(profile=None, endpoint=None, region=None, acl=None):
    s3_additional_kwargs = None
    if acl is not None:
        s3_additional_kwargs = {
            "ACL": acl
        }
    return s3fs.S3FileSystem(
        anon=False,
        client_kwargs={
            "endpoint_url": endpoint,
            "region_name": region
        },
        profile=profile,
        s3_additional_kwargs=s3_additional_kwargs
    )


class Archive(object):

    def __init__(self):
        pass

    def names(
        self,
        src="",
        dest="",
        input_compression="",
        input_name="",
        input_s3_profile="",
        input_s3_endpoint="",
        input_s3_region="",
        output_compression="",
        output_s3_profile="",
        output_s3_endpoint="",
        output_s3_region="",
        input_format="",
        output_format="",
        drop_blanks=False,
        drop_nulls=False,
        limit=None,
    ):

        if src is None or len(src) == 0:
            raise Exception("src is missing")

        if dest is None or len(dest) == 0:
            raise Exception("dest is missing")

        if input_compression is not None and len(input_compression) > 0:
            if input_compression not in algorithms:
                raise Exception(
                    "input_compression is invalid: only the following compression algorithms are supported: {}".format(
                        ", ".join(algorithms)
                    )
                )

        if output_compression is not None and len(output_compression) > 0:
            if output_compression not in algorithms:
                raise Exception(
                    "output_compression is invalid: only the following compression algorithms are supported: {}".format(
                        ", ".join(algorithms)
                    )
                )

        if output_format is None or len(output_format) == 0:
            raise Exception("output_format is missing")

        file_systems = {}

        src_path = None
        input_file_system = None
        if src.startswith("s3://"):
            profile = input_s3_profile or os.getenv("AWS_PROFILE") or None
            file_systems[profile] = {}
            endpoint = input_s3_endpoint or None
            file_systems[profile][endpoint] = {}
            region = input_s3_region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
            file_systems[profile][endpoint][region] = create_s3_filesystem(
                profile=profile,
                endpoint=endpoint,
                region=region,
                acl=None
            )
            input_file_system = file_systems[profile][endpoint][region]
            src_parts = urlparse(src)
            src_path = "{}{}".format(src_parts.netloc, src_parts.path).removesuffix("/")
        else:
            src_path = src

        dest_path = None
        output_file_system = None
        if dest.startswith("s3://"):
            profile = output_s3_profile or os.getenv("AWS_PROFILE") or None
            if profile not in file_systems:
                file_systems[profile] = {}
            endpoint = output_s3_endpoint or None
            if endpoint not in file_systems[profile]:
                file_systems[profile][endpoint] = {}
            region = output_s3_region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
            if region not in file_systems[profile][endpoint]:
                file_systems[profile][endpoint][region] = create_s3_filesystem(
                    profile=profile,
                    endpoint=endpoint,
                    region=region,
                    acl=None
                )
            output_file_system = file_systems[profile][endpoint][region]
            dest_parts = urlparse(dest)
            dest_path = "{}{}".format(dest_parts.netloc, dest_parts.path).removesuffix("/")
        else:
            dest_path = dest

        data = names(
            src=src_path,
            compression=input_compression,
            fs=input_file_system
        )

        serialize(
            compression=(output_compression or None),
            dest=dest_path,
            data=data,
            format=output_format,
            fs=output_file_system,
            limit=limit
        )


class Athena(object):

    def __init__(self):
        pass

    def query(
        self,
        allow_nan=None,
        drop_blanks=None,
        drop_nulls=None,
        workgroup="",
        query="",
        dest="",
        input_athena_profile="",
        input_athena_endpoint="",
        input_athena_region="",
        output_compression="",
        output_s3_profile="",
        output_s3_endpoint="",
        output_s3_region="",
        input_format="",
        output_format="",
        limit=None,
    ):
        allow_nan = allow_nan or False
        drop_blanks = drop_blanks or False
        drop_nulls = drop_nulls or False

        if workgroup is None or len(workgroup) == 0:
            raise Exception("workgroup is missing")

        if query is None or len(query) == 0:
            raise Exception("query is missing")

        if dest is None or len(dest) == 0:
            raise Exception("dest is missing")

        if output_compression is not None and len(output_compression) > 0:
            if output_compression not in algorithms:
                raise Exception(
                    "output_compression is invalid: only the following compression algorithms are supported: {}".format(
                        ", ".join(algorithms)
                    )
                )

        if output_format is None or len(output_format) == 0:
            raise Exception("output_format is missing")

        dest_path = None
        output_file_system = None
        if dest.startswith("s3://"):
            output_file_system = create_s3_filesystem(
                profile=output_s3_profile or os.getenv("AWS_PROFILE") or None,
                endpoint=output_s3_endpoint or None,
                region=output_s3_region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"),
                acl=None
            )
            dest_parts = urlparse(dest)
            dest_path = "{}{}".format(dest_parts.netloc, dest_parts.path).removesuffix("/")
        else:
            dest_path = dest

        athena_client = pyathena.connect(
            profile_name=input_athena_profile or os.getenv("AWS_PROFILE") or None,
            work_group=workgroup,
            endpoint_url=input_athena_endpoint or None,
            region_name=input_athena_region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"),
            cursor_class=PandasCursor
        )
        athena_cursor = athena_client.cursor()

        data = athena_cursor.execute(query).as_pandas()

        serialize(
            allow_nan=allow_nan,
            compression=(output_compression or None),
            dest=dest_path,
            data=data,
            drop_nulls=drop_nulls,
            drop_blanks=drop_blanks,
            format=output_format,
            fs=output_file_system,
            limit=limit
        )


class CLI(object):

    def __init__(self):
        self.archive = Archive()
        self.athena = Athena()

    def algorithms(self):
        for algorithm in algorithms:
            print(algorithm)

    def formats(self):
        for format in formats:
            print(format)

    def transform(
        self,
        src="",
        dest="",
        input_compression="",
        input_name="",
        input_s3_profile="",
        input_s3_endpoint="",
        input_s3_region="",
        output_compression="",
        output_s3_profile="",
        output_s3_endpoint="",
        output_s3_region="",
        input_format="",
        output_format="",
        drop_blanks=False,
        drop_nulls=False,
        limit=None,
    ):

        if src is None or len(src) == 0:
            raise Exception("src is missing")

        if dest is None or len(dest) == 0:
            raise Exception("dest is missing")

        if src == "<stdin>":
            src = "-"

        if dest == "<stdout>":
            dest = "-"

        if input_compression is not None and len(input_compression) > 0:
            if input_compression not in algorithms:
                raise Exception(
                    "input_compression is invalid: only the following compression algorithms are supported: {}".format(
                        ", ".join(algorithms)
                    )
                )

        if output_compression is not None and len(output_compression) > 0:
            if output_compression not in algorithms:
                raise Exception(
                    "output_compression is invalid: only the following compression algorithms are supported: {}".format(
                        ", ".join(algorithms)
                    )
                )

        if input_format is None or len(input_format) == 0:
            raise Exception("input_format is missing")

        if output_format is None or len(output_format) == 0:
            raise Exception("output_format is missing")

        if input_compression == "zip" and len(input_name) == 0:
            raise Exception("input_name is missing, required when using zip compression")

        file_systems = {}

        src_path = None
        input_file_system = None
        if src.startswith("s3://"):
            profile = input_s3_profile or os.getenv("AWS_PROFILE") or 'None'
            file_systems[profile] = {}
            endpoint = input_s3_endpoint or None
            file_systems[profile][endpoint] = {}
            region = input_s3_region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
            file_systems[profile][endpoint][region] = create_s3_filesystem(
                profile=profile,
                endpoint=endpoint,
                region=region,
                acl=None
            )
            input_file_system = file_systems[profile][endpoint][region]
            src_parts = urlparse(src)
            src_path = "{}{}".format(src_parts.netloc, src_parts.path).removesuffix("/")
        else:
            src_path = src

        dest_path = None
        output_file_system = None
        if dest.startswith("s3://"):
            profile = output_s3_profile or os.getenv("AWS_PROFILE") or None
            if profile not in file_systems:
                file_systems[profile] = {}
            endpoint = output_s3_endpoint or None
            if endpoint not in file_systems[profile]:
                file_systems[profile][endpoint] = {}
            region = output_s3_region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
            if region not in file_systems[profile][endpoint]:
                file_systems[profile][endpoint][region] = create_s3_filesystem(
                    profile=profile,
                    endpoint=endpoint,
                    region=region,
                    acl=None
                )
            output_file_system = file_systems[profile][endpoint][region]
            dest_parts = urlparse(dest)
            dest_path = "{}{}".format(dest_parts.netloc, dest_parts.path).removesuffix("/")
        else:
            dest_path = dest

        data = deserialize(
            src=src_path,
            compression=(input_compression or None),
            format=input_format,
            drop_nulls=drop_nulls or False,
            drop_blanks=drop_blanks or False,
            fs=input_file_system,
            name=input_name or None
        )

        serialize(
            compression=(output_compression or None),
            dest=dest_path,
            data=data,
            format=output_format,
            fs=output_file_system,
            limit=limit
        )


def main():
    fire.Fire(CLI)
