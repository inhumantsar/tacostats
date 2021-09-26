import logging
import pytz

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Generator, List, Union

from praw import Reddit
from praw.models import Submission, Redditor, MoreComments
from praw.reddit import Comment
import prawcore
from prawcore.exceptions import PrawcoreException

from tacostats import statsio
from tacostats.config import REDDIT, USE_EXISTING

CREATE_TIME = time(hour=7, tzinfo=timezone.utc)

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
logging.getLogger("praw").setLevel(logging.WARNING)
logging.getLogger("prawcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

reddit_client = Reddit(**REDDIT)


@dataclass
class DT:
    """Date and Submission"""
    submission: Submission

    @property
    def date(self) -> date:
        return datetime.utcfromtimestamp(self.submission.created_utc).date()        

def get_comment(id: str) -> Comment:
    """Return a PRAW Comment for replying to."""
    return reddit_client.comment(id)
    
def recap(daysago=1) -> Generator[DT, None, None]:
    """Get a dt from the past. Raises a KeyError"""
    target_date = get_target_date(daysago)
    log.info("looking for old dt, target: ", target_date)

    for submission in reddit_client.subreddit("neoliberal").new(limit=None):
        if not submission:
            log.debug(f"submission is falsey: {submission}")
            continue
        if not _is_dt(submission):
            continue 
        dt = DT(submission)
        if target_date == dt.date:
            yield dt

def current() -> Generator[DT, None, None]:
    """Get the current dt by assuming it's one of the current stickies. Raises KeyError."""
    # first sticky might be something else, try both
    for i in [1, 2]:
        try:
            submission = reddit_client.subreddit("neoliberal").sticky(number=i)
        except prawcore.NotFound as e:
            # for some reason not finding a second sticky started causing 404s.
            continue

        if _is_dt(submission):
            yield DT(submission)

def comments(dt: DT) -> Generator[Dict[str, Any], None, None]:
    """Find the appropriate DT and slurp its comments"""
    # existing comments files in s3 have already been processed
    if USE_EXISTING:
        log.info(f"reading comments already stored on s3 for {dt.date}")
        yield from statsio.read_comments(dt_date=dt.date)
    else:
        log.info(f"reading comments direct from reddit for {dt.date}")
        yield from _process_comments(dt, _actually_get_comments(dt.submission))

def _process_comments(dt: DT, comments: List[Comment]) -> Generator[Dict[str, Any], None, None]:
    for comment in comments:
        # _actually_get_comments is supposed to replace all the MoreComments, but this is a backup
        # it should log an error but continue on.
        if isinstance(comment, MoreComments):
            try:
                yield from _process_comments(dt, comment.comments())
            except Exception as e:
                log.exception(f"Unable to process MoreComments obj: ${comment}")
        elif processed := _process_raw_comment(dt, comment):
            yield processed

def _process_raw_comment(dt: DT, comment: Comment) -> Union[None, Dict[str, Any]]:
    """clean up comments and return a dictionary. 
    
    Args:
        comment 
        dt_date     - Used to filter out comments which don't belong to the target dt.
    """
    # recaps end up coming with a handful of comments from the next day
    # need to NOT exclude comments from the next day but before the next dt though
    cdt = datetime.fromtimestamp(comment.created_utc, tz=pytz.utc)
    eodt = datetime.combine(dt.date + timedelta(days=1), CREATE_TIME, tzinfo=pytz.utc)
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
        return {
            "author": author,
            "author_flair_text": comment.author_flair_text,
            "score": comment.score,
            "id": comment.id,
            "permalink": comment.permalink,
            "body": comment.body,
            "created_utc": comment.created_utc,
        }
    except Exception as e:
        log.exception(f"{comment.id}: {e}", exc_info=e)    


def get_target_date(daysago: int) -> date:
    """Returns a past DT's date from N days ago"""
    current_utc = datetime.now().astimezone(timezone.utc)

    # DTs are created by jobautomator automatically at 2ET
    create_date = current_utc.date()

    # need to add an extra day if we're between DTs
    if current_utc.hour < CREATE_TIME.hour:
        daysago += 1

    return (datetime.combine(create_date, CREATE_TIME) - timedelta(days=daysago)).date()


def _is_dt(dt: Submission) -> bool:
    """Runs through a couple tests to be sure it's a DT (or Thunderdome?)"""
    return all([_check_title(dt.title), _check_author(dt.author)])

def _check_author(author: str) -> bool:
    authors = [
        "vhgomes12", "paulatreides0", "ThatFrenchieGuy",
        "CletusMcGuilly", "Buenzlitum", "UrbanCentrist",
        "qchisq", "lionmoose", "cdstephens",
        "dubyahhh", "sir_shivers", "EScforlyfe",
        "p00bix", "dorambor", "iIoveoof",
        "jenbanim", "bd_one", "vivoovix",
        "chatdargent", "jobautomator", "Lux_Stella"
    ]
    result = author in authors
    log.debug(f"_check_author author: {author} result: {result}")
    return result

def _check_title(title: str) -> bool:
    titles = ["discussion thread", "thunderdome", "dôme du tonnerre"]
    result = any([t in title.lower() for t in titles])
    log.debug(f"_check_title title: {title} result: {result}")
    return result

def _actually_get_comments(submission: Submission) -> List[Comment]:
    """Get all comments from submission"""
    log.info("running replace_more... this will take a while...")
    start = datetime.now(timezone.utc)
    submission.comment_sort = "new"
    submission.comment_limit = 1000
    try:
        submission.comments.replace_more(limit=None) # type: ignore
    except AssertionError as ae:
        log.exception(f"AssertionError: {ae}", exc_info=ae)

    log.info(f"done in {(datetime.now(timezone.utc) - start).total_seconds()} seconds")
    return submission.comments.list()


def _blank_comment(comment: Comment) -> dict:
    """Dictifies deleted, removed, and other blanked comments"""
    return {
        "author": "",
        "author_flair_text": "",
        "score": 0,
        "id": comment.id,
        "permalink": comment.permalink,
        "body": comment.body,
        "created_utc": comment.created_utc,
    }


def _get_author_name(author: Union[Redditor, str]) -> str:
    """Author name is a landmine."""
    if not author:
        return ""
    if isinstance(author, str):
        return author
    return author.name
