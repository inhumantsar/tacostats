from collections import Counter
import json
import logging
import re

from datetime import date, datetime, timedelta, timezone
from typing import Optional

import emoji
from jinja2 import Environment, PackageLoader, select_autoescape
import numpy
import pytz

from pandas import DataFrame

from tacostats.config import CREATE_TIME

NEUTER_RE = re.compile(r"!ping", re.MULTILINE | re.IGNORECASE | re.UNICODE)

log = logging.getLogger(__name__)


def get_target_dt_date(daysago: int, date_from: Optional[datetime | date] = None) -> date:
    """Returns a past DT's date from N days ago"""
    if isinstance(date_from, date):
        date_from = datetime.combine(date_from, CREATE_TIME)
    date_from_utc = (date_from or datetime.now()).astimezone(timezone.utc)

    # DTs are created by jobautomator automatically at 2ET
    create_date = date_from_utc.date()

    # need to add an extra day if we're between DTs
    if date_from_utc.hour < CREATE_TIME.hour and date_from_utc.date == datetime.now().date:
        daysago += 1

    return (datetime.combine(create_date, CREATE_TIME) - timedelta(days=daysago)).date()


def find_emoji(body: str) -> list[str]:
    """Returns all of the emoji in a given string in the order that they appear."""
    return [i["emoji"] for i in emoji.emoji_list(body)]


def build_time_indexed_df(df: DataFrame) -> DataFrame:
    """Copies the basic dataframe and indexes it along creation time in EST"""
    tdf = df.copy(deep=True)
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


def render_template(data: dict, template_name: str) -> str:
    """Reads Jinja template in and fills it with `data`, returning the rendered body"""
    jinja_env = Environment(loader=PackageLoader("tacostats"), autoescape=select_autoescape())
    template = jinja_env.get_template(template_name)
    print(f"got template: {template_name} {template}, rendering...")
    return template.render(**data)
