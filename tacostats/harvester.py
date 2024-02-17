import re
import sys


from datetime import date, datetime, timezone

from tacostats.statsio import get_latest_dt_date, write, get_dt_prefix
from tacostats.config import RECAP
from tacostats.reddit.dt import fetch_comments
from tacostats.util import get_target_dt_date

NEUTER_RE = re.compile(r"!ping", re.MULTILINE | re.IGNORECASE | re.UNICODE)


def lambda_handler(event, context):
    harvest_comments()


def harvest_comments(daysago=None):
    """pull dt comments and stash them in s3"""
    start = datetime.now(timezone.utc)
    print(f"harvest_comments started at {start}...")

    dt_date: date
    if RECAP or daysago:
        dt_date = get_target_dt_date(1 if not daysago else daysago)
    else:
        dt_date = get_latest_dt_date()

    print("writing results...")
    write(
        get_dt_prefix(dt_date),
        comments=list(fetch_comments(dt_date)),
    )


if __name__ == "__main__":
    daysago = int(sys.argv[1]) if len(sys.argv) > 1 else None
    harvest_comments(daysago=daysago)
