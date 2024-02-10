from dataclasses import asdict, dataclass, field
import logging
import os
import re

from datetime import date, datetime, timedelta, timezone
from token import OP
from typing import Any, Dict, Generator, Hashable, List, Optional, Tuple, Union

import json
from unittest import result
import numpy
import pandas
import pytz

from pandas import DataFrame
from praw.exceptions import ClientException, RedditAPIException

from tacostats.statsio import write, read_comments, read, get_age
from tacostats.stats import find_top_emoji
from tacostats.models import Comment
from tacostats.openai_api import create_chat_completion
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
USERSTATS_PREFIX = "userstats"
GPT_MODE = os.getenv("GPT_MODE", "false").lower() == "true"
USE_CACHE = os.getenv("USE_CACHE", "true").lower() == "true"


@dataclass
class UserStatsResults:
    comments_per_day: Dict[str, Union[int, float, str]]
    words_per_comment: Dict[str, Union[int, float]]
    top_emoji: Optional[List[List]]
    top_comment: Dict[Hashable, Any]
    average_score: float
    username: str
    span: str
    comments: List[Comment] = field(default_factory=list)
    gpt_prompt: Optional[str] = None
    gpt_response: Optional[str] = None


def lambda_handler(event, context):
    """AWS Lambda handler which receives batched messages from SQS"""
    msg_count = len(event["Records"])
    log.info(f"userstats started. processing {msg_count} messages.")

    # expecting messages like `{username, requester_comment_id, days, ...}`
    for idx, msg in enumerate(event["Records"]):
        data = json.loads(msg["body"])
        username = data["username"]
        days = data["days"]
        comment_id = data["requester_comment_id"]

        log.info(f"({idx}/{msg_count}) ({comment_id}) getting stats for {username} across {days} days...")
        process_userstats(username, comment_id, days)


def process_userstats(username: str, comment_id: str, days: int = 7):
    """dt stats but for a single user"""
    results = None
    if USE_CACHE:
        results = _read_results(username, days)
    if not results:
        results = _build_results(username, days)
    log.info(f"results: {results}")

    # post comment
    template = "userstats.md.j2" if not GPT_MODE else "userstats_gpt.md.j2"
    try:
        reply(asdict(results), template, comment_id)
    except RedditAPIException as e:
        log.exception(f"While trying to reply, Reddit reported an error: {e}")
    except ClientException as e:
        log.exception(f"While trying to reply, PRAW reported an error: {e}")
    except Exception as e:
        log.exception(f"While trying to reply, an unknown exception occurred: {e}")
    else:
        log.info(f"replied to {comment_id}")


def _get_gpt_response(results: UserStatsResults) -> Tuple[str, str]:
    comments_str = "\n".join([f"---\n{c.body}\n" for c in results.comments])
    prompt = f"""
        You are responding to u/{results.username}. The following is a summary of their activity over the last {results.span}.
        They wrote {results.comments_per_day['max']} comments in a single day, 
        {results.comments_per_day['mean']} comments per day on average with an average score of {results.average_score}.
        These are the emoji they used along with total count of each: {results.top_emoji}.
        They use an average of {results.words_per_comment['mean']} words per comment, with a max of {results.words_per_comment['max']}.
        Their most popular comment was written 
        {results.comments_per_day['max_day']} and had a score of {results.top_comment['score']}. That comment was:
        ---
        {results.top_comment['body']}
        ---

        The comments they wrote in the last {results.span} follow.
        {comments_str}
    """
    prompt += """\n\n
            Give the person a recap of their activity. Focus on the comments and sprinkle in one or two of the most extreme statistics. 
            If most of their comments revolve around one topic, try to reference that, especially if it seems like an inside joke.
            
            Address your response to them directly. 
            Take the piss out of them a little but don't spend a lot of time praising or critiquing them.
            Try to form a narrative about their activity, and don't be afraid to be a little snarky.
            Group topics together.
        """
    return (prompt, create_chat_completion(prompt, temperature=1))


def _get_user_prefix(username: str, span: str) -> str:
    return f"{username}-{span.replace(' ', '_')}"


def _get_span(days: int) -> Optional[str]:
    if days == 1:
        return "day"
    if days == 7:
        return "week"
    if days == 30:
        return "month"
    if days > 30:
        return "all time"


def _build_results(username: str, days: int) -> UserStatsResults:
    """read in author comments and return result set"""
    # get all comments by a single author
    comments = list(_generate_author_comments(username, days))
    # yeah... we're swapping a dataclass back to dict right after generating it, but whatever.
    df = DataFrame([c.to_dict() for c in comments])  # type: ignore
    top_emoji = find_top_emoji(df)
    span = _get_span(days) or "week"
    results = UserStatsResults(
        comments_per_day=_get_comments_per_day(df),
        words_per_comment=_get_words_per_comment(df),
        top_emoji=top_emoji[0] if len(top_emoji) > 0 else None,
        top_comment=_get_top_comment(df),
        average_score=_get_average_score(df),
        username=username,
        span=span,
        comments=comments,
    )

    if GPT_MODE:
        # log.info(f"getting GPT response for {username} over the last {span}. results: {results}")
        results.gpt_prompt, results.gpt_response = _get_gpt_response(results)

    write(prefix=USERSTATS_PREFIX, **{_get_user_prefix(username, span): asdict(results)})
    return results


def _read_results(username, days) -> Optional[UserStatsResults]:
    """read in past results if they are fresh enough, returns None if too old or not found"""
    # default to daily allowed refresh...
    max_age = 24 * 60 * 60
    # except for today's stats, allow hourly...
    if days == 1:
        max_age = max_age / 24
    # and for all-time stats, only allow weekly...
    if days > 30:
        max_age = max_age * 7

    user_prefix = _get_user_prefix(username, _get_span(days) or "")
    try:
        if get_age(USERSTATS_PREFIX, user_prefix) < max_age:
            results = UserStatsResults(**read(USERSTATS_PREFIX, user_prefix))
            if GPT_MODE and not results.gpt_response:
                # log.info(f"getting GPT response for {username} over the last {results.span}. results: {results}")
                results.gpt_prompt, results.gpt_response = _get_gpt_response(results)
                write(prefix=USERSTATS_PREFIX, **{_get_user_prefix(username, results.span): asdict(results)})

    except KeyError:
        pass


def _get_top_comment(df: DataFrame) -> Dict[Hashable, Any]:
    """Return comment with most upvotes as a dict with `body`, `score`, and `permalink` keys"""
    return df[["body", "score", "permalink"]].sort_values(by="score", ascending=False).head(1).to_dict("records")[0]


def _get_average_score(df: DataFrame):
    """Get average comment score"""
    return df["score"].mean()


def _get_comments_per_day(df: DataFrame) -> Dict[str, Union[int, float, str]]:
    """Find max and mean comments per day"""
    tdf = build_time_indexed_df(df)
    cpd = tdf.groupby([pandas.Grouper(freq="D")]).size()  # type: ignore
    max_day = cpd[cpd == cpd.max()].index.values[0]  # type: ignore
    return {"max": cpd.max(), "mean": cpd.mean(), "max_day": _get_friendly_date_string(max_day)}


def _get_comments_per_hour(df: DataFrame) -> Dict[str, Union[int, float]]:
    """Find max and mean comments per hour"""
    tdf = build_time_indexed_df(df)
    cph = tdf.groupby([pandas.Grouper(freq="H")]).size()  # type: ignore
    max_hour = cph[cph == cph.max()].index.values[0]  # type: ignore
    return {"max": cph.max(), "mean": cph.mean(), "max_hour": max_hour}


def _get_words_per_comment(df: DataFrame) -> Dict[str, Union[int, float]]:
    """Find max and mean words per comment"""
    wc = df["body"].str.count(" ") + 1
    return {"max": wc.max(), "mean": wc.mean()}


def _generate_author_comments(username: str, days: int) -> Generator[Comment, None, None]:
    """Find all DT comments belonging to a particular user, going back `USERSTATS_HISTORY` days."""
    for dt_date in [get_target_date(i) for i in range(0, days)]:
        yield from _generate_user_comments(username, dt_date=dt_date)


def _generate_user_comments(username: str, dt_date: date) -> Generator[Comment, None, None]:
    """Read comments file for date, filter out a single user's comments."""
    try:
        for comment in read_comments(dt_date):
            if comment.author == username:
                yield comment
    except Exception as e:
        pass


def _get_friendly_date_string(dt64: numpy.datetime64) -> str:
    """Return a print-ready date, relative if recent, absolute if >24hrs ago."""
    # convert numpy datetime to a tz-aware python datetime
    ts = float((dt64 - numpy.datetime64("1970-01-01T00:00:00")) / numpy.timedelta64(1, "s"))
    dt = datetime.utcfromtimestamp(ts).replace(tzinfo=pytz.timezone("US/Eastern"))
    # get days ago and return string
    daysago = (datetime.now(tz=pytz.timezone("US/Eastern")) - dt).days
    if daysago == 0:
        return "today"
    elif daysago == 1:
        return "yesterday"
    else:
        return f'on {dt.strftime("%A")}'


if __name__ == "__main__":
    process_userstats("CletusVonIvermectin", "h3ak825")
