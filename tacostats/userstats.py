import logging
import re

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Generator, List, Tuple, Union

import json
import numpy
import pandas
import pytz

from pandas import DataFrame
from praw.exceptions import ClientException, RedditAPIException

from tacostats import statsio
from tacostats.stats import find_top_emoji
from tacostats.util import build_time_indexed_df
from tacostats.reddit.dt import get_target_date
from tacostats.reddit.report import reply

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
logging.getLogger("praw").setLevel(logging.WARNING)
logging.getLogger("prawcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)


NEUTER_RE = re.compile(r"!ping", re.MULTILINE | re.IGNORECASE | re.UNICODE)
USERSTATS_PREFIX = 'userstats'

def lambda_handler(event, context):
    """AWS Lambda handler which receives batched messages from SQS"""
    msg_count = len(event['Records'])
    log.info(f"userstats started. processing {msg_count} messages.")

    # expecting messages like `{username, requester_comment_id, days, ...}`
    for idx, msg in enumerate(event['Records']):
        data = json.loads(msg['body'])
        username = data['username']
        days = data['days']
        comment_id = data['requester_comment_id']

        log.info(f"({idx}/{msg_count}) ({comment_id}) getting stats for {username} across {days} days...")
        process_userstats(username, comment_id, days)
        

def process_userstats(username: str, comment_id: str, days: int = 7):
    """dt stats but for a single user"""
    max_age = (1 if days == 1 else 24) * 60 * 60
    results = _read_results(username, max_age) or _build_results(username, days)
    log.info(f"results: {results}")

    # post comment
    try:
        reply(results, 'userstats.md.j2', comment_id)
    except RedditAPIException as e:
        log.exception(f"While trying to reply, Reddit reported an error: {e}")
    except ClientException as e:
        log.exception(f"While trying to reply, PRAW reported an error: {e}")
    except Exception as e:
        log.exception(f"While trying to reply, an unknown exception occurred: {e}")
    else:
        log.info(f'replied to {comment_id}')
    

def _build_results(username: str, days: int):
    """read in author comments and return result set"""
    # get all comments by a single author
    df = DataFrame(_generate_author_comments(username, days)) # type: ignore
    top_emoji = find_top_emoji(df)
    results = {
        'comments_per_day': _get_comments_per_day(df),
        'words_per_comment': _get_words_per_comment(df),
        'top_emoji': top_emoji[0] if len(top_emoji) > 0 else None,
        'top_comment': _get_top_comment(df),
        'average_score': _get_average_score(df),
        'username': username
    }
    statsio.write(prefix=USERSTATS_PREFIX, **{username: results})    
    return results

def _read_results(username, max_age):
    """read in past results if they are fresh enough, returns None if too old or not found"""
    if statsio.get_age(USERSTATS_PREFIX, username) < max_age:
        try:
            return statsio.read(USERSTATS_PREFIX, username)
        except KeyError:
            pass

def _get_top_comment(df: DataFrame) -> Dict[str, Any]:
    """Return comment with most upvotes as a dict with `body`, `score`, and `permalink` keys"""
    return df[['body', 'score', 'permalink']].sort_values(by="score", ascending=False).head(1).to_dict('records')[0]

def _get_average_score(df: DataFrame):
    """Get average comment score"""
    return df['score'].mean()

def _get_comments_per_day(df: DataFrame) -> Dict[str, Union[int, float, str]]:
    """Find max and mean comments per day"""
    tdf = build_time_indexed_df(df)
    cpd = tdf.groupby([pandas.Grouper(freq="D")]).size()        # type: ignore
    max_day = cpd[cpd == cpd.max()].index.values[0]             # type: ignore
    return {'max': cpd.max(), 'mean': cpd.mean(), 'max_day': _get_friendly_date_string(max_day)}

def _get_comments_per_hour(df: DataFrame) -> Dict[str, Union[int, float]]:
    """Find max and mean comments per hour"""
    tdf = build_time_indexed_df(df)
    cph = tdf.groupby([pandas.Grouper(freq="H")]).size()        # type: ignore
    max_hour = cph[cph == cph.max()].index.values[0]            # type: ignore
    return {'max': cph.max(), 'mean': cph.mean(), 'max_hour': max_hour}

def _get_words_per_comment(df: DataFrame) -> Dict[str, Union[int, float]]:
    """Find max and mean words per comment"""
    wc = df['body'].str.count(" ") + 1
    return {'max': wc.max(), 'mean': wc.mean()}

def _generate_author_comments(username: str, days: int) -> Generator[Dict[str, Any], None, None]:
    """Find all DT comments belonging to a particular user, going back `USERSTATS_HISTORY` days."""
    for dt_date in [get_target_date(i) for i in range(0, days)]:
        yield from _generate_user_comments(username, dt_date=dt_date)

def _generate_user_comments(username: str, dt_date: date) -> Generator[Dict[str, Any], None, None]:
    """Read comments file for date, filter out a single user's comments."""
    for comment in statsio.read_comments(dt_date):
        if comment['author'] == username:
            yield comment

def _get_friendly_date_string(dt64: numpy.datetime64) -> str:
    """Return a print-ready date, relative if recent, absolute if >24hrs ago."""
    # convert numpy datetime to a tz-aware python datetime
    ts = float((dt64 - numpy.datetime64('1970-01-01T00:00:00')) / numpy.timedelta64(1, 's'))
    dt = datetime.utcfromtimestamp(ts).replace(tzinfo=pytz.timezone('US/Eastern'))
    # get days ago and return string
    daysago = (datetime.now(tz=pytz.timezone('US/Eastern')) - dt).days
    if daysago == 0:
        return "today"
    elif daysago == 1:
        return "yesterday"
    else:
        return f'on {dt.strftime("%A")}'


if __name__ == "__main__":
    process_userstats("inhumantsar", "h3ak825")

