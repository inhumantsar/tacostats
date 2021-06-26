import pytz

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Generator, List, Union

from praw import Reddit
from praw.models import Submission, Redditor
from praw.reddit import Comment

from tacostats.config import REDDIT, EXCLUDED_AUTHORS, USE_EXISTING
from tacostats import io

CREATE_TIME = time(hour=7, tzinfo=timezone.utc)

reddit_client = Reddit(**REDDIT)


@dataclass
class DT:
    """Date and Submission"""
    submission: Submission

    @property
    def date(self) -> date:
        return datetime.utcfromtimestamp(self.submission.created_utc).date()        


def recap(daysago=1) -> DT:
    """Get a dt from the past. Raises a KeyError"""
    target_date = _build_target_date(daysago)
    print("looking for old dt, target: ", target_date)

    QUERY = 'title:"Discussion Thread" author:jobautomator'
    for submission in reddit_client.subreddit("neoliberal").search(QUERY, sort="new"):
        if not submission:
            print(f"submission is falsey: {submission}")
        dt = DT(submission)
        if target_date == dt.date:
            return dt
    raise KeyError("No DT Found!")


def current() -> DT:
    """Get the current dt by assuming it's one of the current stickies. Raises KeyError."""
    # first sticky might be something else, try both
    for i in [1, 2]:
        submission = reddit_client.subreddit("neoliberal").sticky(number=i)
        if _is_dt(submission):
            return DT(submission)
    raise KeyError("No DT Found!")


def comments(dt: DT) -> Generator[Dict[str, Any], None, None]:
    """Find the appropriate DT and slurp its comments"""
    print(f"getting comments...")
    # existing comments files in s3 have already been processed
    if USE_EXISTING:
        print(f"reading comments already stored on s3 for {dt.date}")
        yield from io.read_s3(prefix_date=dt.date)
    else:
        print(f"reading comments direct from reddit for {dt.date}")
        for comment in _actually_get_comments(dt.submission):
            if processed := _process_raw_comment(comment, dt):
                yield processed


def _process_raw_comment(comment: Comment, dt: DT) -> Union[None, Dict[str, Any]]:
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
        print(f"comment too new: {comment} {cdt}")
        return None

    # author is a touchy field
    author = _get_author_name(comment.author)

    # skip over comments by bots, etc
    # if author in EXCLUDED_AUTHORS:
    #     return None

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
        print(f"{comment.id}: {e}")    

def _build_target_date(daysago: int) -> date:
    """Returns a date in the past to look for"""
    current_utc = datetime.now().astimezone(timezone.utc)

    # DTs are created by jobautomator automatically at 2ET
    create_date = current_utc.date()

    # need to add an extra day if we're between DTs
    if current_utc.hour < CREATE_TIME.hour:
        daysago += 1

    return (datetime.combine(create_date, CREATE_TIME) - timedelta(days=daysago)).date()


def _is_dt(dt: Submission) -> bool:
    """Runs through a couple tests to be sure it's a DT (or Thunderdome?)"""
    return all(
        [
            dt.title == "Discussion Thread" or dt.title.lower().contains("thunderdome"),
            dt.author == "jobautomator",  # add mod list?
        ]
    )


def _actually_get_comments(submission: Submission) -> List[Comment]:
    """Get all comments from submission"""
    print("running replace_more... this will take a while...")
    start = datetime.now(timezone.utc)
    submission.comment_sort = "new"
    submission.comment_limit = 1000
    submission.comments.replace_more(limit=None) # type: ignore
    print(f"done in {(datetime.now(timezone.utc) - start).total_seconds()} seconds")
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
