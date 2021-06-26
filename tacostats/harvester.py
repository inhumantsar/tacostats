import re

from tacostats.config import RECAP

from datetime import datetime, timezone

from tacostats import io
from tacostats.reddit.dt import current, recap, comments

NEUTER_RE = re.compile(r"!ping", re.MULTILINE | re.IGNORECASE | re.UNICODE)


def lambda_handler(event, context):
    harvest_comments()


def harvest_comments():
    """pull dt comments and stash them in s3"""
    start = datetime.now(timezone.utc)
    print(f"harvest_comments started at {start}...")

    dt = recap() if RECAP else current()
    dt_comments = comments(dt)

    print("writing results...")
    io.write(
        dt.date.strftime("%Y-%m-%d"),
        comments=list(dt_comments),
    )

if __name__ == "__main__":
    harvest_comments()