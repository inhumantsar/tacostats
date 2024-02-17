import logging
import pytz

from datetime import date, datetime, timedelta, timezone
from typing import Generator, List, Optional, Union

from backoff import on_exception, expo
from praw import Reddit
from praw.models import Submission, Redditor, MoreComments
from praw.reddit import Comment as RedditComment
import prawcore
from prawcore.exceptions import PrawcoreException

from tacostats.config import CREATE_TIME, REDDIT, THUNDERDOME_TITLES
from tacostats.util import get_target_dt_date
from tacostats.models import Comment


log = logging.getLogger(__name__)

reddit_client = Reddit(**REDDIT)


def get_submission_dt_date(submission: Submission) -> date:
    """Get the date of a submission."""
    return datetime.utcfromtimestamp(submission.created_utc).date()


def get_comment(id: str) -> RedditComment:
    """Return a PRAW Comment for replying to."""
    return reddit_client.comment(id)


@on_exception(lambda: expo(15), PrawcoreException, max_tries=8)
def get_parent_id(id: str) -> str:
    """Return a PRAW Comment's parent Comment."""
    parent: RedditComment | Submission = reddit_client.comment(id).parent()
    # praw strips the prefix from ids, so let's put it back on top-level comments
    if isinstance(parent, Submission):
        return f"t3_{parent.id}"
    else:
        return f"t1_{parent.id}"


def fetch_dt(dt_date: date) -> Generator[Submission, None, None]:
    """Get a DT (and Thunderdome if applicable) from the past. Raises a KeyError"""
    log.info(f"looking for old dt, target: {dt_date}")
    for submission in reddit_client.subreddit("neoliberal").new(limit=None):
        if not submission:
            log.debug(f"submission is falsey: {submission}")
            continue
        if not _is_dt(submission):
            continue
        if dt_date == get_submission_dt_date(submission):
            yield submission

    raise KeyError(f"no dt found for {dt_date}")


def fetch_current_dt() -> Generator[Submission, None, None]:
    """Get the current dt by assuming it's one of the current stickies. Raises KeyError."""
    # first sticky might be something else, try both
    for i in [1, 2]:
        try:
            submission = reddit_client.subreddit("neoliberal").sticky(number=i)
        except prawcore.NotFound:
            # for some reason not finding a second sticky started causing 404s.
            continue

        if _is_dt(submission):
            yield submission


def fetch_comments(dt_date: date) -> Generator[Comment, None, None]:
    """Find the appropriate DT and slurp its comments. Raises KeyError when the desired DT can't be found."""
    submissions = fetch_dt(dt_date)
    log.info(f"reading comments direct from reddit for {dt_date} ({submissions})")
    for submission in submissions:
        yield from _process_comments(dt_date, _actually_get_comments(submission))


def _process_comments(dt_date: date, comments: List[RedditComment]) -> Generator[Comment, None, None]:
    for comment in comments:
        # _actually_get_comments is supposed to replace all the MoreComments, but this is a backup
        # it should log an error but continue on.
        if isinstance(comment, MoreComments):
            try:
                yield from _process_comments(dt_date, comment.comments())
            except Exception as e:
                log.exception(f"Unable to process MoreComments obj: ${comment}")
        elif processed := _process_raw_comment(dt_date, comment):
            yield processed


def _process_raw_comment(dt_date, comment: RedditComment) -> Optional[Comment]:
    """clean up comments and return a dictionary.

    Args:
        * dt
        * comment
    """
    # recaps end up coming with a handful of comments from the next day
    # need to NOT exclude comments from the next day but before the next dt though
    cdt = datetime.fromtimestamp(comment.created_utc, tz=pytz.utc)
    eodt = datetime.combine(dt_date + timedelta(days=1), CREATE_TIME, tzinfo=pytz.utc)
    if cdt > eodt:
        log.debug(f"comment too new: {comment} {cdt}")
        return None

    # author is a touchy field
    author = _get_author_name(comment.author)

    # deleted, removed, etc comments have no author
    if not author:
        return _blank_comment(comment)

    # yield a proper a comment finally
    try:
        return Comment(
            author=author,
            author_flair_text=comment.author_flair_text,
            score=comment.score,
            id=comment.id,
            permalink=comment.permalink,
            body=comment.body,
            created_utc=comment.created_utc,
            parent_id=comment.parent_id,
        )
    except Exception as e:
        log.exception(f"{comment.id}: {e}", exc_info=e)


def _is_dt(dt: Submission) -> bool:
    """Runs through a couple tests to be sure it's a DT (or Thunderdome?)"""
    return all([_check_title(dt.title), _check_author(dt.author)])


def _check_author(author: str) -> bool:
    authors = [
        "vhgomes12",
        "paulatreides0",
        "ThatFrenchieGuy",
        "CletusMcGuilly",
        "Buenzlitum",
        "UrbanCentrist",
        "qchisq",
        "lionmoose",
        "cdstephens",
        "dubyahhh",
        "sir_shivers",
        "EScforlyfe",
        "p00bix",
        "dorambor",
        "iIoveoof",
        "jenbanim",
        "bd_one",
        "vivoovix",
        "chatdargent",
        "jobautomator",
        "Lux_Stella",
    ]
    result = author in authors
    # log.debug(f"_check_author author: {author} result: {result}")
    return result


def _check_title(title: str) -> bool:
    titles = ["discussion thread"] + THUNDERDOME_TITLES
    result = any([t in title.lower() for t in titles])
    # log.debug(f"_check_title title: {title} result: {result}")
    return result


def _actually_get_comments(submission: Submission) -> List[RedditComment]:
    """Get all comments from submission"""
    log.info("running replace_more... this will take a while...")
    start = datetime.now(timezone.utc)
    submission.comment_sort = "new"
    submission.comment_limit = 1000
    try:
        submission.comments.replace_more(limit=None)  # type: ignore
    except AssertionError as ae:
        log.exception(f"AssertionError: {ae}", exc_info=ae)

    log.info(f"done in {(datetime.now(timezone.utc) - start).total_seconds()} seconds")
    return submission.comments.list()


def _blank_comment(comment: RedditComment) -> Comment:
    """Handles deleted, removed, and other blanked comments"""
    return Comment(
        author="",
        author_flair_text="",
        score=0,
        id=comment.id,
        permalink=comment.permalink,
        body=comment.body,
        created_utc=comment.created_utc,
        parent_id=comment.parent_id,
    )


def _get_author_name(author: Union[Redditor, str]) -> str:
    """Author name is a landmine."""
    if not author:
        return ""
    if isinstance(author, str):
        return author
    return author.name
