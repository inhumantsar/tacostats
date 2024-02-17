import json
import logging

from typing import Any, Dict, List

import boto3
import regex
from tacostats.statsio_backends.base import BaseBackend

from tacostats.util import NumpyEncoder, now
from tacostats.config import COMMENTS_KEY, S3_BUCKET

log = logging.getLogger(__name__)

PREFIX_REGEX = regex.compile(r"\d{4}-\d{2}-\d{2}")

S3_BUCKET_NOT_SET_ERROR = "S3_BUCKET not set"


class S3Backend(BaseBackend):

    @staticmethod
    def write(prefix: str, **kwargs):
        """write data to s3.

        Args:
            prefix - s3 "path" to write to. must not include trailing slash.
            kwargs - key is s3 "filename" to write, value is json-serializable data.
        """
        if not S3_BUCKET:
            raise ValueError(S3_BUCKET_NOT_SET_ERROR)

        for key, value in kwargs.items():
            s3_key = f"{prefix}/{key}.json"
            log.debug(f"writing to {s3_key}")
            boto3.client("s3").put_object(Body=str(json.dumps(value, cls=NumpyEncoder)), Bucket=S3_BUCKET, Key=s3_key)

    @staticmethod
    def read_comments(prefix: str) -> List[Dict[str, Any]]:
        return S3Backend.read(prefix, COMMENTS_KEY)

    @staticmethod
    def read(prefix: str, key: str) -> Any:
        """Read json data stored in bucket."""
        if not S3_BUCKET:
            raise ValueError(S3_BUCKET_NOT_SET_ERROR)
        path = f"{prefix}/{key}.json"
        log.debug(f"reading from {path}")
        s3 = boto3.client("s3")
        try:
            json_str = s3.get_object(Bucket=S3_BUCKET, Key=path)
        except s3.exceptions.NoSuchKey as e:
            raise KeyError(e)

        return json.loads(json_str["Body"].read().decode())

    @staticmethod
    def get_age(prefix: str, key: str):
        """get number of seconds since object was last modified"""
        if not S3_BUCKET:
            raise ValueError(S3_BUCKET_NOT_SET_ERROR)

        objects = boto3.client("s3").list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{prefix}/{key}")

        if objects["KeyCount"] == 0:
            raise KeyError(f"Unable to find an object matching {prefix}/{key}*")
        elif objects["KeyCount"] > 1:
            log.warning(f"{objects['KeyCount']} objects found matching {prefix}/{key}*, ignoring all but the first.")

        return now() - int(objects["Contents"][0]["LastModified"].timestamp())

    @staticmethod
    def get_listing() -> List[str]:
        if not S3_BUCKET:
            raise ValueError(S3_BUCKET_NOT_SET_ERROR)

        s3_listing = boto3.client("s3").list_objects_v2(
            Bucket=S3_BUCKET,
            Delimiter="/",
            # TODO: worry about this in 3 years
            MaxKeys=1000,
        )["CommonPrefixes"]
        log.info(s3_listing)
        return s3_listing
