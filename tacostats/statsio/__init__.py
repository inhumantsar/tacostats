from datetime import date
from typing import Any, Dict, List, Union
from tacostats.config import COMMENTS_KEY, DRY_RUN, LOCAL_STATS
from tacostats.statsio import local, s3
from tacostats.statsio.s3 import get_dt_prefix

def write(prefix: str, **kwargs):
    if LOCAL_STATS:
        local.write(prefix, **kwargs)
    if DRY_RUN:
        print('dry run enabled. skipping s3 write.')
    else:
        s3.write(prefix, **kwargs)

def read(prefix: str, key: str) -> Any:
    """Read json file. Raises KeyError if the requested file can't be found."""
    if LOCAL_STATS:
        return local.read(prefix, key)
    return s3.read(prefix, key=key)

def read_comments(dt_date: Union[None, date] = None) -> List[Dict[str, Any]]:
    """Read comments file"""
    return read(get_dt_prefix(dt_date), COMMENTS_KEY)

def read_user_comments(username: str, dt_date: Union[None, date] = None) -> List[Dict[str, Any]]:
    """Read user comments for a particular day"""
    return read(get_dt_prefix(dt_date), username)
