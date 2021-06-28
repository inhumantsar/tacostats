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
    emojis = emoji.UNICODE_EMOJI_ENGLISH.keys()
    # \X matches graphemes, ie: regular chars as well as combined chars like letter+ligature or 👨‍👩‍👦‍👦
    matches = regex.findall(r"\X", body)
    return [i for i in matches if i in emojis]

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


def from_utc_to_est(created_utc) -> datetime:
    as_utc = datetime.fromtimestamp(created_utc, tz=timezone.utc)
    return as_utc.astimezone(pytz.timezone("US/Eastern"))

