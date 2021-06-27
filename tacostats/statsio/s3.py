import json
import regex

from datetime import date
from typing import Any, Dict, List, Union

import boto3

from tacostats.config import COMMENTS_KEY, S3_BUCKET

_S3_CLIENT = boto3.client("s3")
_PREFIX_REGEX = regex.compile(r"\d{4}-\d{2}-\d{2}")

def write(prefix: str, **kwargs):
    """write data to s3.
    
    Args:
        prefix - s3 "path" to write to. must not include trailing slash.
        kwargs - key is s3 "filename" to write, value is json-serializable data.
    """
    print("writing to s3...")
    for key, value in kwargs.items():
        _S3_CLIENT.put_object(
            Body=str(json.dumps(value)), Bucket=S3_BUCKET, Key=f"{prefix}/{key}.json"
        )

def read_comments(dt_date: Union[date, None]) -> List[Dict[str, Any]]:
    return read(get_dt_prefix(dt_date), COMMENTS_KEY)

def read(prefix: str, key: str) -> Any:
    """Read json data stored in bucket."""
    print(f"reading {key} from s3 at {prefix}...")
    json_str = _S3_CLIENT.get_object(Bucket=S3_BUCKET, Key=f"{prefix}/{key}.json")["Body"].read().decode()
    return json.loads(json_str)

def get_dt_prefix(dt_date: Union[date,None] = None) -> str:
    """Format dt s3 prefix using date. grabs the latest from s3 if no date is provided."""
    if dt_date:
        return dt_date.strftime("%Y-%m-%d")
    else:
        return get_latest_dt_prefix()

def get_latest_dt_prefix() -> str:
    s3_listing = _S3_CLIENT.list_objects_v2(
        Bucket='tacostats-data',
        Delimiter='/',
        MaxKeys=1000,
    )['CommonPrefixes']
    filtered = [i['Prefix'] for i in s3_listing if _PREFIX_REGEX.math(i['Prefix'])]
    prefix = sorted(filtered, reverse=True)[0]
    print(f"got latest daily s3 prefix: {prefix}")
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
