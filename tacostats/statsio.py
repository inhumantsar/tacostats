from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime
from json import JSONDecodeError
import logging
from typing import Any, Dict, Generator, Iterable, List, Optional, Union

import regex

from tacostats.config import COMMENTS_KEY, USE_LOCAL, USE_S3
from tacostats.statsio_backends import BaseBackend, S3Backend, LocalBackend
from tacostats.models import Comment, Thread
from tacostats.reddit.dt import get_parent_id
from tacostats.util import get_target_dt_date

PREFIX_REGEX = regex.compile(r"\d{4}-\d{2}-\d{2}")
PREFIX_DATE_FORMAT = "%Y-%m-%d"

log = logging.getLogger(__name__)


@dataclass
class CommentsIndex:
    comments_by_id: Dict[str, Comment] = field(default_factory=dict)
    comment_ids_by_dt_date: Dict[date, List[str]] = field(default_factory=dict)
    comment_ids_by_author: Dict[str, List[str]] = field(default_factory=dict)
    comment_ids_by_parent: Dict[str, List[str]] = field(default_factory=dict)

    @property
    def comments(self) -> List[Comment]:
        return list(self.comments_by_id.values())

    @property
    def size(self) -> int:
        return len(self.comments_by_id)

    def get_by_ids(self, id_list: Iterable[str]) -> List[Comment]:
        return [self.comments_by_id[id] for id in id_list if id in self.comments_by_id]

    def get_top_level_parent(self, comment: Comment) -> Comment:
        """Find the top-level parent of any given comment. Returns the input comment if it's already top-level."""
        if not comment.parent_id or (not comment.parent_id.startswith("t1_") and not comment.parent_id.startswith("t3_")):
            raise ValueError(f"comment {comment.id} has an invalid or no parent_id")
        # t3 indicates the parent is a submission, thus the comment is a top-level comment
        elif comment.parent_id.startswith("t3_"):
            return comment
        # t1 indicates the parent is another comment, so we need to move up the chain
        else:
            return self.get_top_level_parent(self.comments_by_id[comment.parent_id[3:]])

    def get_thread(self, comment: Comment, parent_thread: Optional[Thread] = None) -> Thread:
        """Get the entire thread for any given comment."""
        log.debug(f"building thread for comment {comment.id}")
        # move up to the top level parent if we're not already there
        c = comment if parent_thread else self.get_top_level_parent(comment)
        log.debug(f"top level parent for comment {comment.id} is {c.id}")

        # recursively build child threads
        thread = Thread(comment=c, parent=parent_thread)
        for child_id in self.comment_ids_by_parent.get(c.id, []):
            thread.children.append(self.get_thread(self.comments_by_id[child_id], thread))

        return thread

    def index_comments(self, comments: Iterable[Comment]):
        """Index comments for easy access."""
        for comment in comments:
            self.comments_by_id[comment.id] = comment

            if comment.author not in self.comment_ids_by_author:
                self.comment_ids_by_author[comment.author] = [comment.id]
            else:
                self.comment_ids_by_author[comment.author].append(comment.id)

            dt_date = comment.created_utc.date()
            if dt_date not in self.comment_ids_by_dt_date:
                self.comment_ids_by_dt_date[dt_date] = [comment.id]
            else:
                self.comment_ids_by_dt_date[dt_date].append(comment.id)

            if not comment.parent_id:
                continue

            parent_id = comment.parent_id[3:]
            if parent_id not in self.comment_ids_by_parent:
                self.comment_ids_by_parent[parent_id] = [comment.id]
            else:
                self.comment_ids_by_parent[parent_id].append(comment.id)


class StatsIO:
    _dts: List[str] = []
    _idx: CommentsIndex = CommentsIndex()
    _backends: List[BaseBackend] = []

    def __init__(self) -> None:
        if USE_LOCAL:
            self._backends.append(LocalBackend())
        if USE_S3:
            self._backends.append(S3Backend())
        if len(self._backends) == 0:
            raise ValueError("no backends enabled")

        for dt in sorted(self._backends[0].get_listing(), reverse=True):
            if PREFIX_REGEX.match(dt):
                self._dts.append(dt)

        if not self._dts or len(self._dts) == 0:
            raise KeyError("no dt prefixes found")
        else:
            log.debug(f"found {len(self._dts)} dts")

    @property
    def latest_dt_prefix(self) -> str:
        """Get the latest dt prefix."""
        return self._dts[0]

    @property
    def latest_dt_date(self) -> date:
        """Get the latest dt date."""
        return datetime.strptime(self.latest_dt_prefix, PREFIX_DATE_FORMAT).date()

    def get_age(self, prefix: str, key: str) -> int:
        """Gets the age of a file in seconds. NOTE: Naively uses the age returned from the first available backend."""
        return self._backends[0].get_age(prefix, key)

    def get_dt_dates(self, daysago: int = 0, date_from: Optional[date] = None) -> List[date]:
        """Get dt dates going back N days, optionally starting from a specific date. Defaults to the latest dt available."""
        date_from = date_from or self.latest_dt_date
        log.debug(f"daysago: {daysago}")
        log.debug(f"date_from: {date_from}")
        if daysago:
            return sorted([get_target_dt_date(daysago - i, date_from=date_from) for i in range(1, daysago + 1)], reverse=True)
        else:
            return [date_from]

    def get_dt_prefix(self, dt_date: Union[date, None] = None) -> str:
        """Format dt s3 prefix using date. grabs the latest from storage if no date is provided."""
        # it's possible that there might not be a current for the dt, so fail gracefully by just
        # grabbing the latest one
        if dt_date:
            return dt_date.strftime(PREFIX_DATE_FORMAT)
        else:
            return self.latest_dt_prefix

    def _process_dt_dates_arg(self, dt_dates: Optional[date | List[date]] = None) -> List[date]:
        """Processes a method's dt_dates argument into a list of dates, defaulting to the latest available date."""
        dt_dates = dt_dates or self.latest_dt_date
        if isinstance(dt_dates, date):
            dt_dates = [dt_dates]
        return dt_dates

    def read(self, prefix: str, key: str) -> Any:
        """
        Reads data from storage. Will iterate through each backend until a match is found.

        Raises KeyError if the requested file can't be found in any backend.
        """
        # quietly try backends until we find a match or run out of backends
        results = None
        for backend in self._backends:
            try:
                results = backend.read(prefix, key)
                break
            except (FileNotFoundError, KeyError):
                log.warning(f"file not found in {backend}: {prefix}/{key}")
            except JSONDecodeError as e:
                log.error(f"error decoding {prefix}/{key} from {backend}: {e}")

        if not results:
            raise KeyError(f"unable to load any results for: {prefix}/{key}")

        return results

    def _read_comments(self, dt_date: date, username: Optional[str] = None) -> Generator[Comment, None, None]:
        """Returns comments from one DT. Defaults to latest DT, optionally filtered by username."""
        self.update_index(dt_date)
        comment_ids = self._idx.comment_ids_by_dt_date[dt_date]
        if username:
            comment_ids = set(self._idx.comment_ids_by_author[username]) & set(comment_ids)

        yield from self._idx.get_by_ids(comment_ids)

    def read_comments(self, dt_dates: Optional[date | List[date]] = None, username: Optional[str] = None) -> Generator[Comment, None, None]:
        """Returns comments from one or more DTs, optionally filtered by username. Defaults to the latest DT."""
        for d in self._process_dt_dates_arg(dt_dates):
            try:
                yield from self._read_comments(d, username)
            except KeyError:
                log.warning(f"no comments found for {d}{' by ' + username if username else ''}")

    def _read_threads(self, dt_date: date, username: Optional[str] = None) -> Generator[Thread, None, None]:
        self.update_index(dt_date)

        _default: List[str] = []  # can't set a type on `[]` in the get() calls below, so this is used instead

        # create a list of comment_ids and filter if necessary
        comment_ids = self._idx.comment_ids_by_dt_date.get(dt_date, _default)
        if username:
            author_comment_ids = self._idx.comment_ids_by_author.get(username, _default)
            comment_ids = list(set(comment_ids) & set(author_comment_ids))

        # iterate through the comment_ids and yield the threads, skipping any that already appeared in a thread
        processed_ids = set()
        for comment_id in comment_ids:
            if comment_id in processed_ids:
                continue
            try:
                thread = self._idx.get_thread(self._idx.comments_by_id[comment_id])
            except ValueError as e:
                log.warning(f"skipping comment {comment_id}: {e}")
                processed_ids.update([comment_id])
                continue

            processed_ids.update(thread.get_comment_ids())
            yield thread

    def read_threads(self, dt_dates: Optional[date | List[date]] = None, username: Optional[str] = None) -> Generator[Thread, None, None]:
        """Returns threads from one or more DTs, optionally filtered by username. Defaults to the latest DT."""

        for dt_date in self._process_dt_dates_arg(dt_dates):
            yield from self._read_threads(dt_date, username)

    def read_thread(self, comment_id: str, dt_date: date) -> Thread:
        """Returns a single thread for a given comment id."""
        self.update_index(dt_date)
        return self._idx.get_thread(self._idx.comments_by_id[comment_id])

    def update_index(self, dt_date: date):
        if dt_date not in self._idx.comment_ids_by_dt_date:
            comments = [Comment.from_dict(c) for c in self.read(self.get_dt_prefix(dt_date), COMMENTS_KEY)]
            self._idx.index_comments(comments)

    def _update_parent_id(self, comment: Comment) -> Comment:
        parent_id = None
        if parent_id := get_parent_id(comment.id):
            if parent_id.startswith("t1_"):
                log.debug(f"comment {comment.id} is a threaded from comment id {parent_id}")
            else:
                log.debug(f"comment {comment.id} appears to be top-level on submission id {parent_id}")

            comment.parent_id = parent_id
        else:
            log.warning(f"no parent id found for {comment.id}. this should never happen.")

        return comment

    def update_parent_ids(self, dt_date: Optional[date] = None):
        """Update parent_ids for comments. NOTE: This should only be used for manual backfilling."""
        log.info(f"updating parent_ids for {dt_date or self.latest_dt_date}...")
        comments = list(self.read_comments(dt_date))
        counter = 0
        max_counter = len(comments)
        for comment in comments:
            if comment.parent_id:
                log.debug(f"comment {comment.id} already has a parent_id: {comment.parent_id}")
            else:
                comment = self._update_parent_id(comment)
                self.write(dt_prefix=self.get_dt_prefix(dt_date), **{COMMENTS_KEY: [c.to_dict() for c in comments]})
            if counter % 100 == 0:
                log.info(f"{counter/max_counter:.2%} processed")
            counter += 1
        log.info(f"done: {counter} comments")

    def write(self, dt_prefix: str, **kwargs):
        """Write data to all enabled storage backends. kwargs keys are used for file name, values for data."""
        for b in self._backends:
            b.write(dt_prefix, **kwargs)
