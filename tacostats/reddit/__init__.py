from datetime import datetime
from typing import Tuple

from praw import Reddit
from praw.models import Submission

from tacostats.config import REDDIT

reddit_client = Reddit(**REDDIT)