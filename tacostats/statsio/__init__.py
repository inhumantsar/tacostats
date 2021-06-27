from typing import Any, Dict, List
from tacostats.config import COMMENTS_KEY, DRY_RUN, LOCAL_STATS
from tacostats.statsio import local, s3
from tacostats.statsio.s3 import get_dt_prefix

def write(prefix: str = "", **kwargs):
    if LOCAL_STATS:
        local.write(**kwargs)
    if DRY_RUN:
        print('dry run enabled. skipping s3 write.')
    elif prefix:
        s3.write(prefix, **kwargs)
    else:
        print("DRY_RUN is off but prefix isn't set. not writing to s3.")

def read(key: str, prefix: str = "") -> Any:
    """Read json file"""
    if prefix:
        return s3.read(prefix, key=key)
    else:
        raise IOError(f"Unable to read {key}. No S3 prefix is set")

def read_comments(prefix: str = "") -> List[Dict[str, Any]]:
    """Read comments file"""
    return read(COMMENTS_KEY, prefix=prefix or get_dt_prefix())

