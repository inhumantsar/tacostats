import logging
import re
import sys

from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import date, datetime, timezone

import pandas

from pandas import DataFrame, Series
from scipy import stats
from tacostats.statsio import StatsIO
from tacostats.config import EXCLUDED_AUTHORS, RECAP
from tacostats.reddit import report
from tacostats.reddit.dt import fetch_comments
from tacostats.models import Comment
from tacostats.util import build_time_indexed_df, find_emoji, get_target_dt_date, neuter_ping

_FLAIRMOJI_REGEX = re.compile(r".*(\:[\-\w]+\:)\s(.*)")

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
logging.getLogger("praw").setLevel(logging.WARNING)
logging.getLogger("prawcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

statsio = StatsIO()


def lambda_handler(event, context):
    process_stats()


def process_stats(daysago: Optional[int] = None):
    """pull basic statistics from a thread's comments"""
    start = datetime.now(timezone.utc)
    print(f"process_stats started at {start}...")

    dt_date: date
    if RECAP or daysago:
        dt_date = get_target_dt_date(1 if not daysago else daysago)
    else:
        dt_date = statsio.latest_dt_date

    dt_comments = fetch_comments(dt_date)

    # TODO: no longer necessary?
    # during thunderdomes, calling comments() twice often results in duplicated
    # comments. reworking the way comments are written to s3 would be a proper fix
    # but deduping this way is simpler and not too high-cost.
    # if len(dts) > 1:
    #     dt_comments = [dict(t) for t in {tuple(sorted(d.items())) for d in dt_comments}]

    print("processing comments...")
    full_stats, short_stats = _process_comments(dt_comments)

    print("writing results...")
    statsio.write(
        statsio.get_dt_prefix(dt_date),
        full_stats=full_stats,
        short_stats=short_stats,
    )

    print("posting results...")
    report.post(short_stats, "template.md.j2")

    done = datetime.now(timezone.utc)
    duration = (done - start).total_seconds()
    print(f"Finished at {done.isoformat()}, took {duration} seconds")


def _process_comments(dt_comments: Iterable[Comment]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """build a full dataset and a short dataset from a list of comments"""
    cdf = DataFrame([c.to_dict() for c in dt_comments])  # type: ignore

    print("removing bot comments...")
    # pandas syntax is dumb so pylance (rightly) thinks this returns a series
    cdf: DataFrame = cdf[~cdf.author.isin(EXCLUDED_AUTHORS)]  # type: ignore

    print("counting bad authors...")
    deleted, removed, other = _find_bad_author_counts(cdf)

    print("pruning bad authors from dataframe...")
    cdf = cdf[cdf["author"] != ""]

    print("adding derived columns...")
    cdf["emoji_count"] = cdf["body"].apply(lambda x: len(find_emoji(x)))
    cdf["word_count"] = cdf["body"].str.count(" ") + 1

    # prep time-indexed dataframe
    print("creating time-indexed dataframe...")
    tdf = build_time_indexed_df(cdf)

    # build new dataframes used in other metrics
    print("creating spammiest dataframe...")
    spammiest = _find_spammiest(cdf)
    print("creating wordiest dataframe...")
    wordiest = _find_wordiest(cdf)
    print("creating upvoted_redditors dataframe...")
    upvoted_redditors = _find_upvoted_redditors(cdf)
    print("creating unique_users dataframe...")
    unique_users = _find_unique_users(cdf)

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
        "upvoted_comments": [neuter_ping(c) for c in _find_upvoted_comments(cdf)],
        "upvoted_redditors": upvoted_redditors.to_dict("records"),
        "best_redditors": _find_avg_scores(upvoted_redditors, spammiest),
        # "memeiest": memeiest_full,
        "activity": _find_activity_by_hour(tdf),
        "hourly_wordiest": _find_wordiest_by_hour(tdf),
        "hourly_spammiest": _find_spammiest_by_hour(tdf),
        "emoji_spammers": _find_emoji_spammers(cdf),
        "top_emoji": find_top_emoji(cdf),
        "unique_users": len(unique_users),
        "flair_population": _find_flair_population(unique_users),
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
            short_stats[k] = v[: min(len(v), 150)]
        else:
            # print(f"{k} found, truncating to top 3")
            short_stats[k] = v[:3]

    return short_stats


def _extract_flairmoji(flair_text):
    if not flair_text:
        return ""

    try:
        matches = _FLAIRMOJI_REGEX.match(flair_text)
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
    return groups[1]


def _find_flair_population(unique_users_df):
    flairs = unique_users_df["author_flair_text"].apply(_extract_flairmoji).value_counts()
    flair_list = [i for i in zip(flairs, flairs.index) if i[1]]
    unflaired_count = int(flairs.at[""])
    r = {"unflaired": unflaired_count, "flaired": flair_list}
    return r


def _find_unique_users(df):
    return df[["author", "author_flair_text"]].drop_duplicates(["author", "author_flair_text"])


def _find_wordiest_per_comment(wordiest: DataFrame, spammiest: DataFrame) -> List[dict]:
    """Find the users who used the most words per comment.

    Returns:
        [{'author': str, 'author_flair_text': str, 'avg_words': float}, ...]
    """
    df = pandas.merge(wordiest, spammiest, on="author")

    # add average words column
    df["avg_words"] = (df["word_count"] / df["comment_count"]).round(decimals=1)

    return df[["author", "author_flair_text", "avg_words"]].sort_values("avg_words", ascending=False).to_dict("records")


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
    avg_score_df["avg_score"] = (avg_score_df["score"] / avg_score_df["comment_count"]).round(decimals=1)

    return avg_score_df[["author", "avg_score"]].sort_values("avg_score", ascending=False).to_dict("records")


def _find_upvoted_redditors(cdf: DataFrame) -> DataFrame:
    """Find the users who have collected the most upvotes

    Returns:
        DataFrame["author", "score"]
    """
    return cdf[["author", "score"]].groupby("author").sum().reset_index().sort_values(["score"], ascending=False)


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
    print(f"found {deleted} deleted, {removed} removed, {other} other bad authors.")
    return deleted, removed, other


def _find_activity_by_hour(tdf: DataFrame) -> List[float]:
    """Get a normalized activity indicator for each one-hour span.

    Returns:
        [0<float<1, ...]
    """
    activity = tdf[["word_count"]].groupby(pandas.Grouper(freq="H")).agg(["sum", "count"])  # type: ignore
    maximums = activity.max()["word_count"]
    minimums = activity.min()["word_count"]
    activity["norm_count"] = (activity["word_count"]["count"] - minimums["count"]) / (maximums["count"] - minimums["count"])
    return list(activity["norm_count"].to_dict().values())


def _find_wordiest_by_hour(tdf: DataFrame) -> List[dict]:
    """Find the users who wrote the most words within each one-hour span.

    Returns:
        [{'created_at': int, 'author': str, 'word_count': float}, ...]
    """
    activity = tdf[["word_count", "author"]].groupby([pandas.Grouper(freq="H"), "author"]).sum()  # type: ignore
    activity = activity[activity == activity.groupby(level=0).transform("max")].dropna().reset_index()
    activity["created_et"] = activity["created_et"].apply(lambda x: x.value / 10**9).astype(int)
    return activity.to_dict("records")


def _find_spammiest_by_hour(tdf: DataFrame) -> List[dict]:
    """Find the users who posted the most often within each one-hour span.

    Returns:
        [{'created_et': val, 'author': val, 'comment_count': val}, ...]
    """
    spammiest_s: Series[int] = tdf.groupby([pandas.Grouper(freq="H"), "author"]).size()  # type: ignore
    d = {}
    for k, v in spammiest_s.iteritems():  # type: ignore # TODO: why does this complain about spammiest_s being an int?
        dt = int(k[0].value / 10**9)  # type: ignore
        if dt not in d.keys() or d[dt][1] < v:
            d[dt] = (k[1], v)  # type: ignore
    return [{"created_et": k, "author": v[0], "comment_count": v[1]} for k, v in d.items()]


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


def find_top_emoji(cdf: DataFrame) -> List[List]:
    """Returns a list of the most used emoji

    Returns:
        [[<count>, <emoji>], ...]
    """
    top_emoji = cdf["body"].apply(find_emoji)
    log.info(top_emoji)
    top_emoji = top_emoji.where(top_emoji.str.len() > 0).dropna().explode().value_counts()  # type: ignore
    log.info(top_emoji)
    return list(zip(top_emoji, top_emoji.index))  # type: ignore


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


if __name__ == "__main__":
    daysago = int(sys.argv[1]) if len(sys.argv) > 1 else None
    process_stats(daysago=daysago)
