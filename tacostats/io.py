import json
import os
import pathlib

from datetime import datetime, timedelta
from pprint import pprint
from tacostats import reddit

import boto3

from tacostats.config import DRY_RUN, LOCAL_STATS, S3_BUCKET


def write(prefix, **kwargs):
    if prefix and not DRY_RUN:
        write_s3(prefix, **kwargs)
    write_local(**kwargs)


def write_s3(prefix, **kwargs):
    print("writing to s3...")
    s3 = boto3.client("s3")
    for key, value in kwargs.items():
        s3.put_object(
            Body=str(json.dumps(value)), Bucket=S3_BUCKET, Key=f"{prefix}/{key}.json"
        )


def write_local(**kwargs):
    if not LOCAL_STATS:
        print("LOCAL_STATS is falsey, skipping local write.")
        return

    print("writing local...")
    # _check_for_unserializable_shit(kwargs)
    for key, value in kwargs.items():
        with open(f"{key}.json", "w", encoding="utf-8") as fh:
            fh.write(json.dumps(value))


def read_local(key):
    with open(f"{key}.json", encoding="utf-8") as fh:
        return json.loads(fh.read())


def _check_for_unserializable_shit(value):
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
