import boto3
import logging
import os
import sys

from datetime import timezone, time
from distutils.util import strtobool

from dotenv import load_dotenv

load_dotenv(verbose=True)

# set up the root logger and silence noisy modules
log_level_name = os.getenv("LOG_LEVEL", "INFO")  # Default to INFO if not set
LOG_LEVEL = getattr(logging, log_level_name.upper())
logging.getLogger().setLevel(LOG_LEVEL)
logging.basicConfig(level=LOG_LEVEL, format="%(levelname)s:%(module)s.%(funcName)s: %(message)s")
logging.getLogger("praw").setLevel(logging.WARNING)
logging.getLogger("prawcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.INFO)

log = logging.getLogger(__name__)

secrets = boto3.client("secretsmanager")
get_secret = lambda x: secrets.get_secret_value(SecretId=x)["SecretString"]

# filenames shared among all modules, storage module handles extension
COMMENTS_KEY = "comments"
FULLSTATS_KEY = "full_stats"
KEYWORDS_KEY = "keywords"

# data bucket
S3_BUCKET = os.getenv("S3_BUCKET")
log.info(f"S3_BUCKET         {S3_BUCKET}")

USE_S3 = bool(strtobool(os.getenv("WRITE_S3", "False")))
log.info(f"WRITE_S3          {USE_S3}")

WRITE_REDDIT = bool(strtobool(os.getenv("WRITE_REDDIT", "False")))
log.info(f"WRITE_REDDIT      {WRITE_REDDIT}")

# write stats locally as well as to s3
USE_LOCAL = bool(strtobool(os.getenv("LOCAL_STATS", "False")))
log.info(f"LOCAL_STATS       {USE_LOCAL}")

# only use what already exists in s3
USE_EXISTING = bool(strtobool(os.getenv("USE_EXISTING", "False")))
log.info(f"USE_EXISTING      {USE_EXISTING}")

# if true, look at yesterday's stats
RECAP = bool(strtobool(os.getenv("RECAP", "False")))
log.info(f"RECAP             {RECAP}")

# the time new dt's are posted
CREATE_TIME = time(hour=7, tzinfo=timezone.utc)
log.info(f"CREATE_TIME       {CREATE_TIME}")

# use cached results if they exist -- only for userstats atm
USE_CACHE = bool(strtobool(os.getenv("USE_CACHE", "True")))
log.info(f"USE_CACHE         {USE_CACHE}")

# use openai to generate response -- only for userstats atm
GPT_MODE = bool(strtobool(os.getenv("GPT_MODE", "False")))
log.info(f"GPT_MODE          {GPT_MODE}")

# how far back into a user's DT history to compile stats on
USERSTATS_HISTORY = int(os.getenv("USERSTATS_HISTORY", 7))
log.info(f"USERSTATS_HISTORY {USERSTATS_HISTORY}d")

# openai model to use for chat completions -- only for userstats atm
CHAT_MODEL = os.getenv("CHAT_MODEL", "gpt-4-turbo-preview")
log.info(f"CHAT_MODEL        {CHAT_MODEL}")

# openai model max token count to use for chat completions -- only for userstats atm
MAX_TOKENS = int(os.getenv("MAX_TOKENS", "128000"))
log.info(f"MAX_TOKENS        {MAX_TOKENS}")

# openai model to use for embeddings -- not in use atm
EMBEDDING_MODEL = "text-embedding-3-small"
log.info(f"EMBEDDING_MODEL   {EMBEDDING_MODEL}")

# default temperature for chat completions
DEFAULT_TEMPERATURE = float(os.getenv("DEFAULT_TEMPERATURE", 0.5))
log.info(f"DEFAULT_TEMPERATURE {DEFAULT_TEMPERATURE}")

# prompts
### deprecated text
# There are "ping groups" which are groups people join to discuss related topics.
# Some ping groups have obscure or meme names. Some are listed below, but this may not be exhaustive.
# CANUCKS 	        Canadian memes and shitposting
# MAMADAS 	        Latin American shitposting
# VODKA 	            Pan-Slavic shitposting
# WHATSAPP-FORWARDS 	Indian Shitposting
# YUROP 	            European Shitposting
# COP 	            Kamala Harris
# DIAMOND-JOE 	    Joe Biden memes.
# CUBE 	            High-density housing shitposting
# HUDDLED-MASSES      Immigration & open borders shitposting
# PIDGIN 	            Senate Judicial Committee discussion
# SNEK 	            For the libertarian-minded
# SOYBOY 	            Vegan shitposting
# PRETENTIOUS 	    Prog Metal, Prog Rock, Math Rock, Post Rock and such
# ALPHABET-MAFIA 	    Lighthearted LGBT discussion and shitposting
# FEDORA 	            Atheist, Agnostic, and Irreligious discussions
# Emoji are ok but don't use more than a few.

PRIMER_PROMPT = """
Your name is tacostats. You are a robot-taco and you spend all your time on Reddit. 
You will be interating with people in the daily general discussion thread of a subreddit called r/neoliberal. 
Most people will be from the US, but there are many from other countries as well, especially Canada, Australia, the UK, and Europe.
"""
log.info(f'PRIMER_PROMPT\n"""\n{PRIMER_PROMPT}\n"""')

# Being snarky is good in small doses, don't try to be too cool or too clever or too funny.

RULES_PROMPT = """
THE FOLLOWING ARE IMPORTANT RULES FOR YOUR INTERACTIONS:
Do not use any formatting in responses except for line breaks. 
If you try to copy an emoji, DO NOT write them as escape codes like \\ud83d\\ude24.
Avoid trying to count words or characters on your own.
ALWAYS ROUND NUMBERS UP TO THE NEAREST INTEGER -- this is very important.
Always sign the end of a response with a taco emoji, don't use your name.
If you are going to use an author name or username, write it as u/<username>.
DO NOT REPEAT YOURSELF
"""

log.info(f'RULES_PROMPT\n"""\n{RULES_PROMPT}\n"""')


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
    "tacograph",
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
