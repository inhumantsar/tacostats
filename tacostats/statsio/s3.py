import json
import logging
import regex

from datetime import date
from typing import Any, Dict, List, Union

import boto3
import numpy

from boto3 import exceptions

from tacostats import util
from tacostats.config import COMMENTS_KEY, S3_BUCKET, get_storage_prefix

_PREFIX_REGEX = regex.compile(r"\d{4}-\d{2}-\d{2}")

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
logging.getLogger("praw").setLevel(logging.WARNING)
logging.getLogger("prawcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        if isinstance(obj, numpy.floating):
            return float(obj)
        if isinstance(obj, numpy.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)


def write(prefix: str, **kwargs):
    """write data to s3.

    Args:
        prefix - s3 "path" to write to. must not include trailing slash.
        kwargs - key is s3 "filename" to write, value is json-serializable data.
    """
    for key, value in kwargs.items():
        s3_key = f"{prefix}/{key}.json"
        log.info(f"writing to s3: {s3_key}")
        boto3.client("s3").put_object(Body=str(json.dumps(value, cls=NumpyEncoder)), Bucket=S3_BUCKET, Key=s3_key)


def read_comments(dt_date: Union[date, None]) -> List[Dict[str, Any]]:
    return read(get_dt_prefix(dt_date), COMMENTS_KEY)


def read(prefix: str, key: str) -> Any:
    """Read json data stored in bucket."""
    print(f"reading {key} from s3 at {prefix}...")
    s3 = boto3.client("s3")
    try:
        json_str = s3.get_object(Bucket=S3_BUCKET, Key=f"{prefix}/{key}.json")
    except s3.exceptions.NoSuchKey as e:
        raise KeyError(e)

    return json.loads(json_str["Body"].read().decode())


def get_age(prefix: str, key: str):
    """get number of seconds since object was last modified"""
    objects = boto3.client("s3").list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{prefix}/{key}")

    if objects["KeyCount"] == 0:
        raise KeyError(f"Unable to find an object matching {prefix}/{key}*")
    elif objects["KeyCount"] > 1:
        log.warning(f"{objects['KeyCount']} objects found matching {prefix}/{key}*, ignoring all but the first.")

    return util.now() - int(objects["Contents"][0]["LastModified"].timestamp())


def get_dt_prefix(dt_date: Union[date, None] = None) -> str:
    """Format dt s3 prefix using date. grabs the latest from s3 if no date is provided."""
    if dt_date:
        return get_storage_prefix(dt_date)
    else:
        return get_latest_dt_prefix()


def get_latest_dt_prefix() -> str:
    s3_listing = boto3.client("s3").list_objects_v2(
        Bucket="tacostats-data",
        Delimiter="/",
        # TODO: worry about this in 3 years
        MaxKeys=1000,
    )["CommonPrefixes"]
    filtered = [i["Prefix"] for i in s3_listing if _PREFIX_REGEX.math(i["Prefix"])]
    prefix = sorted(filtered, reverse=True)[0]
    log.debug(f"got latest daily s3 prefix: {prefix}")
    return prefix


def _check_for_unserializable_shit(value):
    """only enabled during debugging. ignore me."""
    if isinstance(value, list):
        for i in value:
            _check_for_unserializable_shit(i)
    elif isinstance(value, dict):
        for k, v in value.items():
            _check_for_unserializable_shit(k)
            _check_for_unserializable_shit(v)
    else:
        try:
            json.dumps(value)
        except Exception as e:
            print(f"GOTCHA: {value}")
            raise (e)
