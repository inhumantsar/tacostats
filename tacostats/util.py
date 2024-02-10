from collections import Counter
import json
import re

from datetime import date, datetime, timezone

import emoji
import numpy
import pytz
import regex  # supports grapheme search

from pandas import DataFrame

NEUTER_RE = re.compile(r"!ping", re.MULTILINE | re.IGNORECASE | re.UNICODE)


def find_emoji(body: str) -> list:
    """Returns all of the emoji, including combined characters, in a string"""
    return emoji.emoji_list(body)


def build_time_indexed_df(df: DataFrame) -> DataFrame:
    """Copies the basic dataframe and indexes it along creation time in EST"""
    tdf = df.copy(deep=True)
    print(tdf.columns)
    tdf["created_utc"] = tdf["created_utc"].apply(from_utc_to_est)
    tdf.rename(columns={"created_utc": "created_et"}, inplace=True)
    tdf.set_index("created_et", inplace=True)
    return tdf


def neuter_ping(comment):
    comment["body"] = NEUTER_RE.sub("*ping", comment["body"])
    return comment


def from_utc_to_est(created_utc: datetime) -> datetime:
    as_utc = datetime.fromtimestamp(created_utc.timestamp(), tz=timezone.utc)
    return as_utc.astimezone(pytz.timezone("US/Eastern"))


def now() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        if isinstance(obj, numpy.floating):
            return float(obj)
        if isinstance(obj, numpy.ndarray):
            return obj.tolist()
        if isinstance(obj, datetime):
            return obj.timestamp()
        return super(NumpyEncoder, self).default(obj)
