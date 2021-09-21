import re


from datetime import datetime, timezone
from itertools import chain

from tacostats import statsio
from tacostats.config import RECAP
from tacostats.reddit.dt import current, recap, comments

NEUTER_RE = re.compile(r"!ping", re.MULTILINE | re.IGNORECASE | re.UNICODE)


def lambda_handler(event, context):
    harvest_comments()

def harvest_comments():
    """pull dt comments and stash them in s3"""
    start = datetime.now(timezone.utc)
    print(f"harvest_comments started at {start}...")

    dts = list(recap() if RECAP else current())
    
    dt_comments = list(chain(*[comments(dt) for dt in dts]))

    print("writing results...")
    statsio.write(
        statsio.get_dt_prefix(dts[0].date),
        comments=dt_comments,
    )

if __name__ == "__main__":
    harvest_comments()