from datetime import date
import json
import os

from pathlib import Path
from tacostats.config import COMMENTS_KEY
from typing import Any, Dict, List, Union

LOCAL_PATH = ".local_stats"

def write(**kwargs):
    """wrote local stats files. use kwargs keys for name, values for data"""
    print("writing local...")
    Path(LOCAL_PATH).mkdir(parents=True, exist_ok=True)
    for key, value in kwargs.items():
        path = os.path.join(LOCAL_PATH, f"{key}.json")
        # _check_for_unserializable_shit(value)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(value))

def read(key: str) -> Any:
    """read local stats file"""
    path = os.path.join(LOCAL_PATH, f"{key}.json")
    with open(path, encoding="utf-8") as fh:
        return json.loads(fh.read())

def read_comments() -> List[Dict[str, Any]]:
    """read local comments file"""
    return read(COMMENTS_KEY)

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
