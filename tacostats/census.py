import json
import re

from datetime import datetime, timedelta
from pprint import pprint

import boto3
import pandas

from tacostats import io


# flair stats
# _, comments, _, _ = reddit.get_comments(force_cache=True)
# cdf = pandas.DataFrame(comments)

_REGEX = re.compile(r'.*(\:[\-\w]+\:)')
_BUCKET = 'tacostats-data'
_DAY_RANGE = 7

def _pull_comments(days=_DAY_RANGE):
    client = boto3.client('s3')
    today = datetime.now()
    prefixes = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]
    for p in prefixes:
        result = client.get_object(Bucket=_BUCKET, Key=f"{p}/comments.json") 
        yield json.loads(result["Body"].read().decode())


def _extract_flairmoji(flair_text):
    if not flair_text: return ""

    try:
        matches = _REGEX.match(flair_text)
    except Exception as e:
        print(e, flair_text)
        return ""

    if not matches:
        # print(f"WARN: regex returned none: {flair_text}")
        return ""

    groups = matches.groups()
    if len(groups) == 0:
        print(f"WARN: no matches: {groups}")
        return ""
    if len(groups) > 1:
        print(f"WARN: >1 matches: {groups}")
    return groups[0]


def _build_dataframe():
    dfs = [pandas.DataFrame(i, columns=i[0].keys()) for i in _pull_comments()]
    return pandas.concat(dfs, ignore_index=True)

def _flairs(df):
    return df['author_flair_text'].apply(_extract_flairmoji).value_counts()

def _unique_users(df):
    return df[['author', 'author_flair_text']].drop_duplicates(['author', 'author_flair_text'])

def _queer_flairs(df):
    queer_flairs = [':bi:', ':gay:', ':ace:', ':trans:', ':enby:', ':lesbian:', ':genderqueer:']
    return df[queer_flairs]

def _build_stats():
    df = _build_dataframe()
    unique_users = _unique_users(df)
    flairs = _flairs(unique_users)
    queer = _queer_flairs(flairs)

    return {
        'unique_users': {'total': len(unique_users), 'data': unique_users.to_dict()},
        'flair_census': flairs.to_dict(),
        'queer_census': {'total': queer.sum(), 'data': queer.to_dict()},
    }

def _write_stats():
    data = {datetime.now().strftime('%U'): _build_stats()}
    pprint(data)
    io.write('census', **data)
    return data

def _publish_comment():


if __name__ == "__main__":
    data = _write_stats()