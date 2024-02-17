from ast import mod
from asyncio import threads
from dataclasses import asdict, dataclass, field
import logging
import os
from random import randint
import re

from datetime import date, datetime, timedelta, timezone
from tabnanny import check
from token import OP
from typing import Any, Dict, Generator, Hashable, List, Optional, Tuple, Union

import json
from unittest import result
from h11 import Data
import jinja2
import numpy
import pandas
import pytz

from pandas import DataFrame
from praw.exceptions import ClientException, RedditAPIException

from tacostats.statsio import StatsIO

# from tacostats.stats import find_top_emoji
from tacostats.models import Comment, Thread
from tacostats.openai_api import MaxTokensExceededError, create_chat_completion, estimate_tokens
from tacostats.config import CHAT_MODEL, GPT_MODE, MAX_TOKENS, USE_CACHE
from tacostats.util import build_time_indexed_df, render_template

# from tacostats.reddit.report import reply


log = logging.getLogger(__name__)

NEUTER_RE = re.compile(r"!ping", re.MULTILINE | re.IGNORECASE | re.UNICODE)
USERSTATS_PREFIX = "userstats"

statsio = StatsIO()


@dataclass
class UserStatsResults:
    comments_per_day: Dict[str, Union[int, float, str]]
    words_per_comment: Dict[str, Union[int, float]]
    top_emoji: Optional[List[List]]
    top_comment: Dict[Hashable, Any]
    average_score: float
    username: str
    span: str
    comments: List[Comment] = field(default_factory=list)  # deprecated
    threads: List[Thread] = field(default_factory=list)
    gpt_prompt: Optional[str] = None  # deprecated
    gpt_response: Optional[str] = None
    overall_stats: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "comments_per_day": self.comments_per_day,
            "words_per_comment": self.words_per_comment,
            "top_emoji": self.top_emoji,
            "top_comment": self.top_comment,
            "average_score": self.average_score,
            "username": self.username,
            "span": self.span,
            # "comments": None,  # deprecated
            # "threads": [t.to_dict() for t in self.threads],
            # "gpt_prompt": None,  # deprecated
            "gpt_response": self.gpt_response,
        }


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
        log.info(f"cache hit: {username} / {days}.")
        results = _read_results(username, days)
    if not results:
        log.info(f"cache miss: {username} / {days}. building results...")
        results = _build_results(username, days)
    log.debug(f"results: {results}")

    # post comment
    template = "userstats.md.j2" if not GPT_MODE else "userstats_gpt_response.md.j2"
    try:
        print(f"replying to {comment_id}")
        # reply(asdict(results), template, comment_id)
    except RedditAPIException as e:
        log.exception(f"While trying to reply, Reddit reported an error: {e}")
    except ClientException as e:
        log.exception(f"While trying to reply, PRAW reported an error: {e}")
    except Exception as e:
        log.exception(f"While trying to reply, an unknown exception occurred: {e}")
    else:
        log.info(f"replied to {comment_id}")


def _sample_threads(thread_sizes: List[Tuple[str, int]]) -> Generator[Tuple[str, int], None, None]:
    tokens = 0
    while tokens < MAX_TOKENS and len(thread_sizes) > 0:
        thread, size = thread_sizes.pop(randint(0, len(thread_sizes) - 1))
        if size + tokens > MAX_TOKENS * 0.85:
            continue

        yield thread, size


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


def _get_daily_stats(days: int) -> Dict[str, Any]:
    """Return a dict of daily stats for the last N days"""
    dt_dates = statsio.get_dt_dates(daysago=days)
    comments = list(statsio.read_comments(dt_dates))
    df = DataFrame([c.to_dict() for c in comments])  # type: ignore
    return {
        "comments_per_day": _get_comments_per_day_by_user(df),
        "words_per_comment": _get_words_per_comment(df),
        "top_comment": _get_top_comment(df),
        "average_score": _get_average_score(df),
    }


def _build_system_prompt(threads_by_score: List[Tuple[str, int]], results: UserStatsResults) -> str:
    """Build a system prompt for GPT-4 based on the top threads"""
    threads_str = "\n".join([t[0] for t in threads_by_score])
    return render_template(
        {
            "threads_str": threads_str,
            "cpd_mean": results.comments_per_day["mean"],
            "overall_cpd_mean": results.overall_stats["comments_per_day"]["mean"],
            "overall_cpd_max": results.overall_stats["comments_per_day"]["max"],
            "top_comment": results.top_comment["body"],
        },
        "userstats_system_prompt.txt.j2",
    )


def _get_gpt_response(results: UserStatsResults, model: str = CHAT_MODEL) -> str:
    # The following is a summary of their activity over the last {results.span}.
    # They wrote {results.comments_per_day['max']} comments in a single day,
    # {results.comments_per_day['mean']} comments per day on average with an average score of {results.average_score}.
    # These are the emoji they used along with total count of each: {results.top_emoji}.
    # They use an average of {results.words_per_comment['mean']} words per comment, with a max of {results.words_per_comment['max']}.
    # Their most popular comment was written
    # {results.comments_per_day['max_day']} and had a score of {results.top_comment['score']}. That comment was:
    # ---
    # {results.top_comment['body']}
    # ---
    # Focus on the comments and sprinkle in one or two of the most extreme statistics.
    # If most of their comments revolve around one topic, try to reference that, especially if it seems like an inside joke.
    #     Try to form a narrative about their activity.
    # Group topics together.
    #     Use the following questions to guide your thinking but don't reference them directly:
    # - What comes to mind when reading their threads?
    # - How would you describe the last {results.span}?
    # - How did they interact with others and how did others respond to them?
    # - Was there a thread (or two) that was especially notable?
    # - Did they post too litte for you to work with?
    #
    # - UNLESS THEY POST SAD THINGS OFTEN, DO NOT HEAP PRAISE OR SUPPORT on people, take the piss a bit and have fun with it.
    # - BE **WAY TOO** NICE TO PEOPLE WHO ARGUE A LOT OR ARE MEAN. Be so sweet it makes them sick.
    # - IF SOMEONE IS ALWAYS NICE, TAKE THE PISS HARD. Rip on them how Jimmy Carr would rip on a heckler.
    # - IF SOMEONE COMPLAINS A LOT, COMPLAIN ABOUT SOMETHING UNRELATED INSTEAD OF TALKING ABOUT THEIR COMMENTS.
    #
    # Look for a recurring theme or pattern in their comments.
    # Keep it SHORT -- 150-200 words MAX.
    threads_by_score = sorted([(t.to_slim_text(), t.get_avg_score()) for t in results.threads], key=lambda x: x[1], reverse=True)
    chat_prompt = render_template({"username": results.username, "span": results.span}, "userstats_chat_prompt.txt.j2")
    system_prompt = _build_system_prompt(threads_by_score, results)
    token_estimate = estimate_tokens(chat_prompt + system_prompt, model)

    # if over MAX_TOKENS, pop low-score threads until we're under to the token limit
    while token_estimate > MAX_TOKENS * 0.8:
        threads_by_score.pop(-1)
        system_prompt = _build_system_prompt(threads_by_score, results)
        token_estimate = estimate_tokens(chat_prompt + system_prompt, model)

    log.info(f"using {len(threads_by_score)}/{len(results.threads)} threads ({token_estimate} tokens) for GPT response.")

    response = ""
    attempts = 0
    while response == "" and attempts < 3:
        try:
            response = create_chat_completion(chat_prompt, system_prompt, temperature=0, model=model)
        except MaxTokensExceededError as exc:
            # this shouldn't be necessary, but just in case
            attempts += 1
            threads_by_score.pop(-1)
            system_prompt = _build_system_prompt(threads_by_score, results)
            log.error(f"{exc}. retrying with {len(threads_by_score)} threads...")

        # check_prompt = (
        #     f"Evaluate whether the following response from GPT4 is an accurate representation of u/{results.username}'s activity."
        # )
        # check_prompt += "\nYour response MUST start either 'Y' for yes or 'N' for no on the first line."
        # check_prompt += "An explanation and recommendations for prompt changes MUST start on the second line."
        # check_prompt += "Your response must less than 200 words long."
        # check_prompt += f"\nu/{results.username}'s activity\n{threads_str}\n\nGPT4's Response:\n{response}"

        # check_response = create_chat_completion(chat_prompt=check_prompt, model=model, temperature=0)
        # if check_response.startswith("no"):
        #     response = ""
        #     log.error("gpt response was rejected. retrying...")

    raise RuntimeError("unable to generate chat completion.")


def _build_results(username: str, days: int) -> UserStatsResults:
    """read in author comments and return result set"""
    # get all comments by a single author
    dt_dates = statsio.get_dt_dates(daysago=days)
    comments = list(statsio.read_comments(dt_dates, username))
    # yeah... we're swapping a dataclass back to dict right after generating it, but whatever.
    df = DataFrame([c.to_dict() for c in comments])  # type: ignore
    top_emoji = []  # find_top_emoji(df) # TODO: Re-enable
    span = _get_span(days) or "week"
    results = UserStatsResults(
        comments_per_day=_get_comments_per_day(df),
        words_per_comment=_get_words_per_comment(df),
        top_emoji=top_emoji[0] if len(top_emoji) > 0 else None,
        top_comment=_get_top_comment(df),
        average_score=_get_average_score(df),
        username=username,
        span=span,
        overall_stats=_get_daily_stats(days),
    )
    log.info(f"results: {results}")

    if GPT_MODE:
        results.threads = list(statsio.read_threads(dt_dates, username))
        results.gpt_response = _get_gpt_response(results)

    statsio.write(dt_prefix=USERSTATS_PREFIX, **{_get_user_prefix(username, span): results.to_dict()})
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
        if statsio.get_age(USERSTATS_PREFIX, user_prefix) < max_age:
            results = UserStatsResults(**statsio.read(USERSTATS_PREFIX, user_prefix))
            if GPT_MODE and not results.gpt_response:
                if not results.threads:
                    log.debug(f"no threads found in stored results for {username}. fetching...")
                    results.threads = list(statsio.read_threads(statsio.get_dt_dates(daysago=days), username))
                _, results.gpt_response = _get_gpt_response(results)
                statsio.write(dt_prefix=USERSTATS_PREFIX, **{_get_user_prefix(username, results.span): asdict(results)})

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


def _get_comments_per_day_by_user(df: DataFrame) -> Dict[str, Union[int, float, str]]:
    """Find max and mean comments per day"""
    tdf = build_time_indexed_df(df)
    cpu = tdf.groupby([pandas.Grouper("author"), pandas.Grouper(freq="D")]).size()  # type: ignore
    return {"max": cpu.max(), "mean": cpu.mean()}


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
    process_userstats("jenbanim", "h3ak825")
