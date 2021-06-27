import re
from typing import Any, Dict, Iterable, List, Tuple

from pandas.core.frame import DataFrame
from tacostats.config import EXCLUDED_AUTHORS, RECAP
import regex  # supports grapheme search

from datetime import datetime, timezone

import emoji
import pandas
import pytz

from tacostats import statsio
from tacostats.reddit import report
from tacostats.reddit.dt import current, recap, comments

NEUTER_RE = re.compile(r"!ping", re.MULTILINE | re.IGNORECASE | re.UNICODE)

def lambda_handler(event, context):
    process_stats()

def process_stats():
    """pull basic statistics from a thread's comments"""
    start = datetime.now(timezone.utc)
    print(f"process_stats started at {start}...")

    dt = recap() if RECAP else current()
    dt_comments = comments(dt)

    print("processing comments...")
    full_stats, short_stats = _process_comments(dt_comments)

    print("writing results...")
    statsio.write(
        statsio.get_dt_prefix(dt.date),
        full_stats=full_stats,
        short_stats=short_stats,
    )

    print("posting results...")
    report.post(short_stats, "template.md.j2")

    done = datetime.now(timezone.utc)
    duration = (done - start).total_seconds()
    print(f"Finished at {done.isoformat()}, took {duration} seconds")


def _process_comments(dt_comments: Iterable[Dict[str, Any]]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """build a full dataset and a short dataset from a list of comments"""
    cdf = pandas.DataFrame(dt_comments) # type: ignore

    print('removing bot comments...')
    # pandas syntax is dumb so pylance (rightly) thinks this returns a series
    cdf: DataFrame = cdf[~cdf.author.isin(EXCLUDED_AUTHORS)] # type: ignore

    print('counting bad authors...')
    deleted, removed, other = _find_bad_author_counts(cdf)

    print('pruning bad authors from dataframe...')
    cdf = cdf[cdf["author"] != ""]

    print("adding derived columns...")
    cdf["emoji_count"] = cdf["body"].apply(lambda x: len(_find_emoji(x)))
    cdf["word_count"] = cdf["body"].str.count(" ") + 1

    # prep time-indexed dataframe
    print("creating time-indexed dataframe...")
    tdf = _build_time_indexed_df(cdf)

    # build new dataframes used in other metrics
    print("creating spammiest dataframe...")
    spammiest = _find_spammiest(cdf)
    print("creating wordiest dataframe...")
    wordiest = _find_wordiest(cdf)
    print("creating upvoted_redditors dataframe...")
    upvoted_redditors = _find_upvoted_redditors(cdf)

    # build stats dicts
    print("building full_stats dict...")
    full_stats = {
        "deleted": deleted,
        "removed": removed,
        "other_blank": other,
        "spammiest": spammiest.to_dict("records"),
        "wordiest_overall": wordiest.to_dict("records"),
        "wordiest": _find_wordiest_per_comment(wordiest, spammiest),
        # neuter upvoted comments to prevent pinging groupbot
        "upvoted_comments": [_neuter_ping(c) for c in _find_upvoted_comments(cdf)],
        "upvoted_redditors": upvoted_redditors.to_dict("records"),
        "best_redditors": _find_avg_scores(upvoted_redditors, spammiest),
        # "memeiest": memeiest_full,
        "activity": _find_activity_by_hour(tdf),
        "hourly_wordiest": _find_wordiest_by_hour(tdf),
        "hourly_spammiest": _find_spammiest_by_hour(tdf),
        "emoji_spammers": _find_emoji_spammers(cdf),
        "top_emoji": _find_top_emoji(cdf),
    }

    return full_stats, _build_short_stats(full_stats)


def _build_short_stats(full_stats: dict) -> dict:
    """truncate any list values to only list the top N entries"""
    print("creating short_stats dict...")
    short_stats = {}
    for k, v in full_stats.items():
        # don't try to truncate anything that's not a list
        if not isinstance(v, list):
            short_stats[k] = v
        elif k.startswith("hourly") or k == "activity":
            # keep all the hourly records
            short_stats[k] = v
        elif k == "top_emoji":
            # keep at most 150 of the top emoji
            short_stats[k] = v[:min(len(v), 150)]
        else:
            # print(f"{k} found, truncating to top 3")
            short_stats[k] = v[:3]

    return short_stats


def _build_time_indexed_df(cdf: DataFrame) -> DataFrame:
    """Copies the basic dataframe and indexes it along creation time in EST"""
    tdf = cdf.copy(deep=True)
    tdf["created_utc"] = tdf["created_utc"].apply(_from_utc_to_est)
    tdf.rename(columns={"created_utc": "created_et"}, inplace=True)
    tdf.set_index("created_et", inplace=True)
    return tdf


def _find_wordiest_per_comment(wordiest: DataFrame, spammiest: DataFrame) -> List[dict]:
    """Find the users who used the most words per comment.

    Returns:
        [{'author': str, 'author_flair_text': str, 'avg_words': float}, ...]
    """
    df = pandas.merge(wordiest, spammiest, on="author")

    # add average words column
    df["avg_words"] = (df["word_count"] / df["comment_count"]).round(decimals=1)

    return (
        df[["author", "author_flair_text", "avg_words"]]
        .sort_values("avg_words", ascending=False)
        .to_dict("records")
    )


def _find_wordiest(cdf: DataFrame) -> DataFrame:
    """Find the users who used the most words overall

    Returns:
        DataFrame["author", "author_flair_text", "word_count"]
    """
    return (
        cdf[["author", "author_flair_text", "word_count"]]
        .groupby("author")
        .sum()
        .reset_index()
        .sort_values(["word_count"], ascending=False)
    )


def _find_avg_scores(upvoted_redditors: DataFrame, spammiest: DataFrame) -> List[dict]:
    """Find the users with the best average upvote score across all their comments

    Returns:
        [{'author': str, 'score': int}, ...]
    """
    avg_score_df = pandas.merge(upvoted_redditors, spammiest, on="author")
    avg_score_df["avg_score"] = (
        avg_score_df["score"] / avg_score_df["comment_count"]
    ).round(decimals=1)

    return (
        avg_score_df[["author", "avg_score"]]
        .sort_values("avg_score", ascending=False)
        .to_dict("records")
    )


def _find_upvoted_redditors(cdf: DataFrame) -> DataFrame:
    """Find the users who have collected the most upvotes

    Returns:
        DataFrame["author", "score"]
    """
    return (
        cdf[["author", "score"]]
        .groupby("author")
        .sum()
        .reset_index()
        .sort_values(["score"], ascending=False)
    )


def _find_upvoted_comments(cdf: DataFrame) -> List[dict]:
    """Find the most highly upvoted comments."""
    return cdf.sort_values(["score"], ascending=False).to_dict("records")


def _find_spammiest(cdf: DataFrame) -> DataFrame:
    """Find the users who posted the most"""
    return (
        cdf[["author", "author_flair_text"]]
        .value_counts()
        .reset_index()
        .rename(columns={0: "comment_count"})
        .sort_values(["comment_count"], ascending=False)
    )


def _find_bad_author_counts(cdf: DataFrame) -> Tuple[int, int, int]:
    """Returns counts of blank messages: `deleted`, `removed`, and `other` in that order"""
    deleted = int(cdf.loc[cdf["body"] == "[deleted]"].size)
    removed = int(cdf.loc[cdf["body"] == "[removed]"].size)
    other = int(cdf.loc[cdf["author"] == ""].size)
    print(f'found {deleted} deleted, {removed} removed, {other} other bad authors.')
    return deleted, removed, other


def _find_activity_by_hour(tdf: DataFrame) -> List[float]:
    """Get a normalized activity indicator for each one-hour span.

    Returns:
        [0<float<1, ...]
    """
    activity = (
        tdf[["word_count"]].groupby(pandas.Grouper(freq="H")).agg(["sum", "count"]) # type: ignore
    )
    maximums = activity.max()["word_count"]
    minimums = activity.min()["word_count"]
    activity["norm_count"] = (activity["word_count"]["count"] - minimums["count"]) / (
        maximums["count"] - minimums["count"]
    )
    return list(activity["norm_count"].to_dict().values())


def _find_wordiest_by_hour(tdf: DataFrame) -> List[dict]:
    """Find the users who wrote the most words within each one-hour span.

    Returns:
        [{'created_at': int, 'author': str, 'word_count': float}, ...]
    """
    activity = (
        tdf[["word_count", "author"]]
        .groupby([pandas.Grouper(freq="H"), "author"]) # type: ignore
        .sum()
    )
    activity = (
        activity[activity == activity.groupby(level=0).transform("max")]
        .dropna()
        .reset_index()
    )
    activity["created_et"] = (
        activity["created_et"].apply(lambda x: x.value / 10 ** 9).astype(int)
    )
    return activity.to_dict("records")


def _find_spammiest_by_hour(tdf: DataFrame) -> List[dict]:
    """Find the users who posted the most often within each one-hour span.

    Returns:
        [{'created_et': val, 'author': val, 'comment_count': val}, ...]
    """
    spammiest_s = tdf.groupby([pandas.Grouper(freq="H"), "author"]).size() # type: ignore
    d = {}
    for k, v in spammiest_s.iteritems():
        dt = int(k[0].value / 10 ** 9) # type: ignore
        if dt not in d.keys() or d[dt][1] < v:
            d[dt] = (k[1], v) # type: ignore
    return [
        {"created_et": k, "author": v[0], "comment_count": v[1]} for k, v in d.items()
    ]


def _find_emoji_spammers(cdf: DataFrame) -> List[dict]:
    """Finds the users who used the most emoji

    Returns:
        [{'author': val, 'emoji_count': val}, ...]
    """
    return (
        cdf[["author", "emoji_count"]]
        .groupby("author")
        .sum()
        .reset_index()
        .sort_values(by="emoji_count", ascending=False)
        .to_dict("records")
    )


def _find_top_emoji(cdf: DataFrame) -> List[list]:
    """Returns a list of the most used emoji

    Returns:
        [[<count>, <emoji>], ...]
    """
    top_emoji = cdf["body"].apply(_find_emoji)
    top_emoji = (
        top_emoji.where(top_emoji.str.len() > 0).dropna().explode().value_counts() # type: ignore
    )
    return list(zip(top_emoji, top_emoji.index)) # type: ignore


def _find_emoji(body: str) -> list:
    """Returns all of the emoji, including combined characters, in a string"""
    emojis = emoji.UNICODE_EMOJI_ENGLISH.keys()
    # \X matches graphemes, ie: regular chars as well as combined chars like letter+ligature or 👨‍👩‍👦‍👦
    matches = regex.findall(r"\X", body)
    return [i for i in matches if i in emojis]


# def _find_memeiest(df):
#     memeiest_terms = ["👆", "👉", "👇", "👈", "☝️"]
#     memeiest_df = (
#         cdf[cdf["body"].str.contains("|".join(memeiest_terms))][["author"]]
#         .value_counts()
#         .reset_index()
#         .rename(columns={0: "meme_count"})
#         .sort_values(["meme_count"], ascending=False)
#     )
#     return memeiest_df.to_dict("records")


def _neuter_ping(comment):
    comment["body"] = NEUTER_RE.sub("*ping", comment["body"])
    return comment


def _from_utc_to_est(created_utc) -> datetime:
    as_utc = datetime.fromtimestamp(created_utc, tz=timezone.utc)
    return as_utc.astimezone(pytz.timezone("US/Eastern"))


if __name__ == "__main__":
    process_stats()
