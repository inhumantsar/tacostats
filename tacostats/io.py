import json

from datetime import date

import boto3

from tacostats.config import DRY_RUN, LOCAL_STATS, S3_BUCKET

_S3_CLIENT = boto3.client("s3")


def write(prefix, **kwargs):
    if LOCAL_STATS:
        write_local(**kwargs)
    if DRY_RUN:
        print('dry run enabled. skipping s3 write.')
    elif prefix:
        write_s3(prefix, **kwargs)
    else:
        print("DRY_RUN is off but prefix isn't set. not writing to s3.")


def write_s3(prefix, **kwargs):
    print("writing to s3...")
    for key, value in kwargs.items():
        _S3_CLIENT.put_object(
            Body=str(json.dumps(value)), Bucket=S3_BUCKET, Key=f"{prefix}/{key}.json"
        )


def write_local(**kwargs):
    print("writing local...")
    for key, value in kwargs.items():
        # _check_for_unserializable_shit(value)
        with open(f"{key}.json", "w", encoding="utf-8") as fh:
            fh.write(json.dumps(value))


def read_s3(prefix_date: date=None, filename='comments.json'):
    """Read data stored in bucket. Defaults to latest comments.json file.

    Args:
        date - `datetime.date` obj or None. If None, find the newest
    """
    prefix = prefix_date.strftime("%Y-%m-%d/") if prefix_date else _get_latest_s3_prefix()
    json_str = _S3_CLIENT.get_object(Bucket=S3_BUCKET, Key=f"{prefix}{filename}")["Body"].read().decode()
    result = json.loads(json_str)
    print(f"got {len(result)} comments from s3")
    return result

def read_local(key):
    with open(f"{key}.json", encoding="utf-8") as fh:
        return json.loads(fh.read())

def _get_latest_s3_prefix():
    """Grab the newest storage prefix. Assumes prefixes are sortable alphabetically. ie: YYYY-MM-DD"""
    s3_listing = _S3_CLIENT.list_objects_v2(
        Bucket='tacostats-data',
        Delimiter='/',
        # EncodingType='url',
        MaxKeys=1000,
        # Prefix='string',
        # ContinuationToken='string',
        # FetchOwner=True|False,
        # StartAfter='string',
        # RequestPayer='requester',
        # ExpectedBucketOwner='string'
    )
    prefixes = [i['Prefix'] for i in s3_listing['CommonPrefixes']]
    return sorted(prefixes, reverse=True)[0]

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
