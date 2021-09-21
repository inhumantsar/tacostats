from datetime import date, datetime
import json
import os

from pathlib import Path
from tacostats.config import COMMENTS_KEY
from typing import Any, Dict, List, Union

LOCAL_PATH = ".local_stats"

def write(prefix: str, **kwargs):
    """wrote local stats files. use kwargs keys for name, values for data"""
    print("writing local...")
    parent = Path(LOCAL_PATH) / prefix
    parent.mkdir(parents=True, exist_ok=True)
    for key, value in kwargs.items():
        path = parent / f"{key}.json"
        # _check_for_unserializable_shit(value)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(value))

def read(prefix: str, key: str) -> Any:
    """read local stats file"""
    path = Path(LOCAL_PATH) / prefix / f"{key}.json"
    print("reading local stats file...")
    with open(path, encoding="utf-8") as fh:
        return json.loads(fh.read())

def read_comments(prefix: str) -> List[Dict[str, Any]]:
    """read local comments file"""
    return read(prefix, COMMENTS_KEY)

def get_age(prefix: str, key: str) -> int:
    """get number of seconds since object was last modified"""
    path = Path(LOCAL_PATH) / prefix / f"{key}.json"
    return int(path.stat().st_mtime - datetime.now().timestamp())

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
