import re

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Generator, List, Tuple, Union

import boto3
import json
import numpy
import pandas
import pytz

from pandas import DataFrame

from tacostats import statsio
from tacostats.stats import find_top_emoji
from tacostats.util import build_time_indexed_df
from tacostats.reddit.dt import get_target_date
from tacostats.reddit.report import reply

NEUTER_RE = re.compile(r"!ping", re.MULTILINE | re.IGNORECASE | re.UNICODE)

def lambda_handler(event, context):
    sqs = boto3.client('sqs')

    # Receive message from SQS queue
    # expecting a message like `{username, requester_comment_id, days}`
    for record in event['Records']:
        data = json.loads(record['body'])
        process_userstats(data['username'], data['requester_comment_id'], data['days'])
        

def process_userstats(username: str, comment_id: str, days: int = 7):
    """dt stats but for a single user"""
    print("starting userstats with", username, days, comment_id)
    # get all comments by a single author
    df = DataFrame(_generate_author_comments(username, days)) # type: ignore
    print(df.count())
    
    top_emoji = find_top_emoji(df)
    results = {    
        'comments_per_day': _get_comments_per_day(df),
        'words_per_comment': _get_words_per_comment(df),
        'top_emoji': top_emoji[0] if len(top_emoji) > 0 else None,
        'top_comment': _get_top_comment(df),
        'average_score': _get_average_score(df),
        'username': username
    }

    # post comment
    print('results:', results)
    reply(results, 'userstats.md.j2', comment_id)

def _get_top_comment(df: DataFrame) -> Dict[str, Any]:
    """Return comment with most upvotes as a dict with `body`, `score`, and `permalink` keys"""
    return df[['body', 'score', 'permalink']].sort_values(by="score", ascending=False).head(1).to_dict('records')[0]

def _get_average_score(df: DataFrame):
    """Get average comment score"""
    return df['score'].mean()

def _get_comments_per_day(df: DataFrame) -> Dict[str, Union[int, float, str]]:
    """Find max and mean comments per day"""
    tdf = build_time_indexed_df(df)
    cph = tdf.groupby([pandas.Grouper(freq="D")]).size() # type: ignore
    return {'max': cph.max(), 'mean': cph.mean(), 'max_day': _get_friendly_max_cph_date(cph)}

def _get_friendly_max_cph_date(cph):
    dt64 = cph[cph == cph.max()].index.values[0]
    ts = (dt64 - numpy.datetime64('1970-01-01T00:00:00')) / numpy.timedelta64(1, 's')
    dt = datetime.utcfromtimestamp(ts).replace(tzinfo=pytz.timezone('US/Eastern'))
    daysago = (datetime.now(tz=pytz.timezone('US/Eastern')) - dt).days
    if daysago == 0:
        return "today"
    elif daysago == 1:
        return "yesterday"
    else:
        return f'on {dt.strftime("%A")}'

def _get_comments_per_hour(df: DataFrame) -> Dict[str, Union[int, float]]:
    """Find max and mean comments per hour"""
    tdf = build_time_indexed_df(df)
    cph = tdf.groupby([pandas.Grouper(freq="H")]).size() # type: ignore
    return {'max': cph.max(), 'mean': cph.mean()}

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


if __name__ == "__main__":
    process_userstats("inhumantsar", "h3ak825")

