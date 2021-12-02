# =================================================================
#
# Work of the U.S. Department of Defense, Defense Digital Service.
# Released as open source under the MIT License.  See LICENSE file.
#
# =================================================================

from urllib.parse import urlparse

import os
import s3fs

from pyserializer.serialize import serialize
from pyserializer.deserialize import deserialize

algorithms = [
    "gzip"
]

formats = [
    "csv",
    "json",
    "jsonl",
    "parquet"
]


def create_s3_filesystem(endpoint=None, region=None, acl=None):
    s3_additional_kwargs = None
    if acl is not None:
        s3_additional_kwargs = {
            "ACL": acl
        }
    return s3fs.S3FileSystem(
        anon=False,
        client_kwargs={
            "endpoint_url": endpoint,
            "region_name": region,
        },
        s3_additional_kwargs=s3_additional_kwargs
    )


class CLI(object):

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
        input_s3_endpoint="",
        input_s3_region="",
        output_compression="",
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

        if input_format is None or len(input_format) == 0:
            raise Exception("input_format is missing")

        if output_format is None or len(output_format) == 0:
            raise Exception("output_format is missing")

        src_path = None
        input_file_system = None
        if src.startswith("s3://"):
            input_file_system = create_s3_filesystem(
                endpoint=input_s3_endpoint or None,
                region=input_s3_region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"),
                acl=None
            )
            src_parts = urlparse(src)
            src_path = "{}{}".format(src_parts.netloc, src_parts.path).removesuffix("/")
        else:
            src_path = src

        dest_path = None
        output_file_system = None
        if dest.startswith("s3://"):
            output_file_system = create_s3_filesystem(
                endpoint=output_s3_endpoint or None,
                region=output_s3_region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"),
                acl=None
            )
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
        )

        serialize(
            compression=(output_compression or None),
            dest=dest_path,
            data=data,
            format=output_format,
            fs=output_file_system,
            limit=limit
        )
