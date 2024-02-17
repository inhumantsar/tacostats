import json
import logging
import os

from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Union

import regex
from tacostats.config import COMMENTS_KEY
from tacostats.util import NumpyEncoder
from tacostats.statsio_backends.base import BaseBackend

LOCAL_PATH = os.getenv("LOCAL_PATH", ".local_stats")

log = logging.getLogger(__name__)


class LocalBackend(BaseBackend):
    @staticmethod
    def write(prefix: str, **kwargs):
        """wrote local stats files. use kwargs keys for name, values for data"""
        parent = Path(LOCAL_PATH) / prefix
        parent.mkdir(parents=True, exist_ok=True)
        for key, value in kwargs.items():
            path = parent / f"{key}.json"
            log.debug(f"writing to {path}")
            # _check_for_unserializable_shit(value)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(json.dumps(value, cls=NumpyEncoder))

    @staticmethod
    def read(prefix: str, key: str) -> Any:
        """read local stats file"""
        path = Path(LOCAL_PATH) / prefix / f"{key}.json"
        log.debug(f"reading from {path}")
        with open(path, encoding="utf-8") as fh:
            return json.loads(fh.read())

    @staticmethod
    def read_comments(prefix: str) -> List[Dict[str, Any]]:
        """read local comments file"""
        return LocalBackend.read(prefix, COMMENTS_KEY)

    @staticmethod
    def get_listing() -> List[str]:
        return [i.name for i in Path(LOCAL_PATH).glob("*") if regex.match(r"\d{4}-\d{2}-\d{2}", i.name)]

    @staticmethod
    def get_age(prefix: str, key: str) -> int:
        """get number of seconds since object was last modified"""
        path = Path(LOCAL_PATH) / prefix / f"{key}.json"
        return int(path.stat().st_mtime - datetime.now().timestamp())
