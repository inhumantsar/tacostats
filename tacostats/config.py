from datetime import date, datetime
import logging
import os

from distutils.util import strtobool

import boto3

from dotenv import load_dotenv

load_dotenv(verbose=True)

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
logging.getLogger("praw").setLevel(logging.WARNING)
logging.getLogger("prawcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)


# data bucket
S3_BUCKET = os.getenv("S3_BUCKET")
print("S3_BUCKET set to ", S3_BUCKET)

WRITE_S3 = bool(strtobool(os.getenv("WRITE_S3", "False")))
print("WRITE_S3 set to ", WRITE_S3)

WRITE_REDDIT = bool(strtobool(os.getenv("WRITE_REDDIT", "False")))
print("WRITE_REDDIT set to ", WRITE_REDDIT)

# write stats locally as well as to s3
LOCAL_STATS = bool(strtobool(os.getenv("LOCAL_STATS", "False")))
print("LOCAL_STATS set to ", LOCAL_STATS)

# only use what already exists in s3
USE_EXISTING = bool(strtobool(os.getenv("USE_EXISTING", "False")))
print("USE_EXISTING set to ", USE_EXISTING)

# if true, look at yesterday's stats
RECAP = bool(strtobool(os.getenv("RECAP", "False")))
print("RECAP set to ", RECAP)

# filenames shared among all modules, storage module handles extension
COMMENTS_KEY = "comments"
FULLSTATS_KEY = "full_stats"
KEYWORDS_KEY = "keywords"

def get_storage_prefix(day: date):
    """Returns a common format for storage prefixes."""
    return day.strftime("%Y-%m-%d")

# how far back into a user's DT history to compile stats on
USERSTATS_HISTORY = int(os.getenv("USERSTATS_HISTORY", 7))

secrets = boto3.client("secretsmanager")
get_secret = lambda x: secrets.get_secret_value(SecretId=x)["SecretString"]

THUNDERDOME_TITLES = ["thunderdome", "dôme du tonnerre", "elefantenrunde"]

REDDIT = {
    "client_id": os.getenv("REDDIT_ID", get_secret("tacostats-reddit-client-id")),
    "client_secret": os.getenv("REDDIT_SECRET", get_secret("tacostats-reddit-client-secret")),
    "user_agent": os.getenv("REDDIT_UA"),
    "username": os.getenv("REDDIT_USER"),
    "password": os.getenv("REDDIT_PASS", get_secret("tacostats-reddit-password")),
}

EXCLUDED_AUTHORS = [
    "jobautomator",
    "AutoModerator",
    "EmojifierBot",
    "EmojifierBotv2",
    "groupbot",
    "tacostats",
    "ShiversifyBot",
    "sorobucksbot",
    "tacograph"
]
CHUNK_TYPES = ["NP", "ADJP"]

BOT_TRIGGERS = [
    "malarkey level",
    "magic goolsball",
    "ping",
]
COMMON_WORDS = [
    "friend",
    "yes",
    "no",
    "difference",
    "couple",
    "type",
    "discussion",
    "users",
    "system",
    "replies",
    "theory",
    "eyes",
    "matter",
    "etc",
    "etc.",
    "area",
    "place",
    "places",
    "image",
    "rest",
    "days",
    "picture",
    "number",
    "guy",
    "guys",
    "others",
    "posts",
    "times",
    "head",
    "support",
    "issues",
    "issue",
    "account",
    "end",
    "line",
    "group",
    "anybody",
    "think",
    "none",
    "side",
    "dt",
    "people",
    "comment",
    "everyone",
    "someone",
    "nothing",
    "something",
    "please",
    "anyone",
    "time",
    "sub",
    "lot",
    "day",
    "country",
    "shit",
    "fuck",
    "thing",
    "good",
    "question",
    "questions",
    "group list",
    "way",
    "thing",
    "things",
    "one",
    "can",
    "moto",
    "ping",
    "anything",
    "reddit",
    "place",
    "today",
    "man",
    "point",
    "life",
    "kind",
    "need",
    "name",
    "stuff",
    "nobody",
    "part",
    "comments",
    "everything",
    "sense",
    "post",
    "lots",
    "sort",
    "bunch",
    "tbh",
    "count",
    "box",
    "half",
    "clue",
    "level",
    "ton",
    "somebody",
    "user",
    "thread",
    "word",
    "dog",
    "thanks",
    "week",
    "problem",
    # "guy",    # i don't really want to hide how male the place is, but these two *dominate* the rankings
    # "guys",   # maybe i can create a group of common subjects and zero just that part of the score?
    "dude",
    "years",
    "bit",
    "try",
    "ones",
    "article",
    "members",  # because "congress" will catch that meaning
    "remember",
    "fine",
    "years",
    "story",
    "words",
    "take",
    "news",
    "cars",  # car will almost always cover that
    "dems",  # ditto democrats
    "hours",
    "minutes",
    "list",
    "topic",
]
STOPWORDS = [
    "i",
    "me",
    "my",
    "myself",
    "we",
    "our",
    "ours",
    "ourselves",
    "you",
    "your",
    "yours",
    "yourself",
    "yourselves",
    "he",
    "him",
    "his",
    "himself",
    "she",
    "her",
    "hers",
    "herself",
    "it",
    "its",
    "itself",
    "they",
    "them",
    "their",
    "theirs",
    "themselves",
    "what",
    "which",
    "who",
    "whom",
    "this",
    "that",
    "these",
    "those",
    "am",
    "is",
    "are",
    "was",
    "were",
    "be",
    "been",
    "being",
    "have",
    "has",
    "had",
    "having",
    "do",
    "does",
    "did",
    "doing",
    "a",
    "an",
    "the",
    "and",
    "but",
    "if",
    "or",
    "because",
    "as",
    "until",
    "while",
    "of",
    "at",
    "by",
    "for",
    "with",
    "about",
    "against",
    "between",
    "into",
    "through",
    "during",
    "before",
    "after",
    "above",
    "below",
    "to",
    "from",
    "up",
    "down",
    "in",
    "out",
    "on",
    "off",
    "over",
    "under",
    "again",
    "further",
    "then",
    "once",
    "here",
    "there",
    "when",
    "where",
    "why",
    "how",
    "all",
    "any",
    "both",
    "each",
    "few",
    "more",
    "most",
    "other",
    "some",
    "such",
    "no",
    "nor",
    "not",
    "only",
    "own",
    "same",
    "so",
    "than",
    "too",
    "very",
    "s",
    "t",
    "can",
    "will",
    "just",
    "don",
    "should",
    "now",
]
