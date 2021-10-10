from datetime import date
from typing import Any, Dict, List, Union
from tacostats.config import COMMENTS_KEY, LOCAL_STATS, WRITE_S3
from tacostats.statsio import local, s3
from tacostats.statsio.s3 import get_dt_prefix

def write(prefix: str, **kwargs):
    if LOCAL_STATS:
        local.write(prefix, **kwargs)
    if not WRITE_S3:
        print('skipping s3 write.')
    else:
        s3.write(prefix, **kwargs)

def read(prefix: str, key: str) -> Any:
    """Read json file. Raises KeyError if the requested file can't be found.
    
    Falls back to S3 if a local match can't be found when LOCAL_STATS==True.
    """
    try:
        if LOCAL_STATS:
            return local.read(prefix, key)
    except FileNotFoundError:
        pass
    return s3.read(prefix, key=key)

def read_comments(dt_date: Union[None, date] = None) -> List[Dict[str, Any]]:
    """Read comments file"""
    return read(get_dt_prefix(dt_date), COMMENTS_KEY)

def read_user_comments(username: str, dt_date: Union[None, date] = None) -> List[Dict[str, Any]]:
    """Read user comments for a particular day"""
    return read(get_dt_prefix(dt_date), username)

def get_age(prefix: str, key: str) -> int:
    if LOCAL_STATS:
        return local.get_age(prefix, key)
    return s3.get_age(prefix, key=key)
