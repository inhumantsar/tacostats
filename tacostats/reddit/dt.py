from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Generator, Union
from tacostats.config import EXCLUDED_AUTHORS

from praw.models import Submission, Redditor
from praw.reddit import Comment

from tacostats.reddit import reddit_client

CREATE_TIME = time(hour=7, tzinfo=timezone.utc)

@dataclass
class DT:
    """Date and Submission"""
    date: date
    submission: Submission

    @staticmethod
    def __init__(self, submission: Submission):
        self.date = datetime.utcfromtimestamp(submission.created_utc).date()
        self.submission = submission

def recap(daysago=1) -> DT:
    """Get a dt from the past. Raises a KeyError"""
    target_date = _build_target_date(daysago)
    print("looking for old dt, target: ", target_date)

    QUERY = 'title:"Discussion Thread" author:jobautomator'
    for submission in reddit_client.subreddit("neoliberal").search(QUERY, sort="new"):
        dt = DT(submission)
        if target_date == dt.date:
            return dt
    raise KeyError("No DT Found!")

def current() -> DT:
    """Get the current dt by assuming it's one of the current stickies. Raises KeyError."""
    # first sticky might be something else, try both
    for i in [1,2]:
        submission = reddit_client.subreddit("neoliberal").sticky(number=i)
        if _is_dt(submission):
            return DT(submission)
    raise KeyError("No DT Found!")

def comments(dt: DT) -> Generator[dict, None, None]:
    """Find the appropriate DT and slurp its comments"""
    print(f"getting comments...")
    for comment in _actually_get_comments(dt.submission):
        # recaps end up coming with a handful of comments from the next day
        # need to NOT exclude comments from the next day but before the next dt though
        cdt = datetime.utcfromtimestamp(comment.created_utc)
        eodt = datetime.combine(dt.date + timedelta(days=1), CREATE_TIME)
        if cdt > eodt:
            print(f"comment too new: {comment} {cdt}")
            continue

        # author is a touchy field
        author = _get_author_name(comment.author)

        # skip over comments by bots, etc
        if author in EXCLUDED_AUTHORS:
            continue

        # deleted, removed, etc comments have no author
        if not author:
            yield _blank_comment(comment)
            continue

        # yield a proper a comment finally
        try:
            yield {
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

def _build_target_date(daysago) -> date:
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
    return all([
        dt.title == "Discussion Thread" or dt.title.lower().contains("thunderdome"),
        dt.author == "jobautomator" # add mod list?
    ])

def _actually_get_comments(submission):
    """find the right dt, replace all the MoreComments objects, and yield"""
    print("running replace_more... this will take a while...")
    start = datetime.now(timezone.utc)
    submission.comment_sort = "new"
    submission.comment_limit = 1000
    submission.comments.replace_more(limit=None)
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


