from itertools import chain
import logging
import os
import re

from datetime import datetime, timedelta, timezone
from io import StringIO
import sys
from typing import Any, Dict, Generator, Iterable, List, Tuple

import contractions
import pandas
import nltk

from markdown import Markdown
from praw.reddit import Comment

from tacostats.statsio import write, get_dt_prefix
from tacostats.reddit import report
from tacostats.reddit.dt import comments, recap, current
from tacostats.config import RECAP, STOPWORDS, COMMON_WORDS, CHUNK_TYPES, BOT_TRIGGERS, EXCLUDED_AUTHORS


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
logging.getLogger("praw").setLevel(logging.WARNING)
logging.getLogger("prawcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)


# before importing pattern need to download the reqd corpora to /tmp, but only in remote Lambda
# for whatever reason, this doesn't appear to be necessary locally, even using sam invoke
if os.getenv("AWS_EXECUTION_ENV", "").startswith("AWS_Lambda"):
    nltk.data.path.append("/tmp")
    for corpus in ["wordnet", "wordnet_ic", "sentiwordnet", "stopwords"]:
        log.debug(f"downloading nltk corpus: {corpus}")
        nltk.download(corpus, download_dir="/tmp")

from pattern.en import parsetree  # type: ignore


def lambda_handler(event, context):
    process_keywords()


def process_keywords(daysago=None):
    """pull keywords out of the dt, score them, and return all but the least significant"""
    start = datetime.now(timezone.utc)
    log.info(f"started at {start}...")

    log.info("getting comments...")
    day = datetime.now().date()
    if RECAP or daysago:
        day = day - timedelta(1 if not daysago else daysago)

    dt_comments = comments(day=day)

    log.info("processing comments...")
    processed = list(_process_comments(dt_comments))
    log.info(f"keyword count: {len(processed)}")
    filtered = [(_format_keyword(i[0]), i[1]) for i in processed if i[1] > 3]
    keywords = {
        "keyword_scores": filtered,
        "keywords_h1": [i[0] for i in filtered[:10]],
        "keywords_h2": [i[0] for i in filtered[10:30]],
        "keywords_h3": [i[0] for i in filtered[30:60]],
        "keywords_h4": [i[0] for i in filtered[60:120]],
        "keywords_h5": [i[0] for i in filtered[120:180]],
        "keywords_h6": [i[0] for i in filtered[180:240]],
    }

    log.info("writing stats...")
    write(get_dt_prefix(day), keywords=keywords)

    log.info("posting comment...")
    report.post(keywords, "keywords.md.j2")

    done = datetime.now(timezone.utc)
    duration = (done - start).total_seconds()
    log.info(f"Finished at {done.isoformat()}, took {duration} seconds")


def _parse_comment(comment: str) -> Generator[Tuple[float, str], None, None]:
    """use pattern to parse keywords out of a comment"""
    comment_str = _clean(comment)
    try:
        parsed = parsetree(
            comment_str,
            tokenize=True,  # Split punctuation marks from words?
            tags=True,  # Parse part-of-speech tags? (NN, JJ, ...)
            chunks=True,  # Parse chunks? (NP, VP, PNP, ...)
            relations=True,  # def: False Parse chunk relations? (-SBJ, -OBJ, ...)
            lemmata=False,  # def: False Parse lemmata? (ate => eat)
            encoding="utf-8",  # Input string encoding.
            tagset=None,
        )
    except Exception as e:
        log.debug(f"\nWARNING: Unable to parse comment: {comment_str}")
        log.debug(e)
        yield (-1, "")
    else:
        for sentence in parsed:
            yield from _parse_sentence(sentence)


def _parse_sentence(sentence) -> Generator[Tuple[float, str], None, None]:
    """flip through each chunk of a sentence, scoring and cleaning it."""
    for chunk in sentence.chunks:
        if (score := _score_chunk(chunk)) > 0:
            if s := _clean_chunk(chunk):
                yield (score, s)


def _clean_chunk(chunk) -> str:
    """remove stopwords from final chunk"""
    goodstr = lambda s: 2 < len(s) < 40 and s not in STOPWORDS
    strings = chunk.string.split(" ")
    return " ".join([i for i in strings if goodstr(i)])


def _score_chunk(chunk) -> float:
    """rates a sentence chunk by the type of word, how many other chunks it relates to, and filters junk words."""
    score = 0
    score += len(chunk.relations) / 2 if len(chunk.relations) > 0 else 0
    score += 1 if chunk.type in CHUNK_TYPES else 0
    score += 1 if chunk.role is not None else 0
    score += 0.5 if len(chunk.modifiers) > 0 else 0

    junk_words = COMMON_WORDS + STOPWORDS + BOT_TRIGGERS
    score = 0 if chunk.head.string in junk_words else score
    score = 0 if any([i in chunk.string for i in BOT_TRIGGERS]) else score
    return score


def _process_comments(comments) -> Generator[Tuple[str, float], None, None]:
    """pull significant keywords from comment list"""
    cdf = pandas.DataFrame(comments)  # type: ignore

    log.debug("removing bot comments...")
    # pandas syntax is dumb so pylance (rightly) thinks this returns a series
    cdf: DataFrame = cdf[~cdf.author.isin(EXCLUDED_AUTHORS)]  # type: ignore

    log.debug(f"got {cdf.count()} comments")

    # parse each comment, sorting the results by score
    parsed_comments = cdf[["body"]].apply(lambda x: [sorted(_parse_comment(c), key=lambda y: y[0], reverse=True) for c in x])

    # explode each result (a list of tuples) into rows of tuples, drop NaNs
    parsed_comments = parsed_comments.explode("body").dropna()

    # create a new dataframe, turning each tuple element into a column and filter common words (why isn't the scoring doing this??)
    keywords_df = pandas.DataFrame(parsed_comments["body"].to_list(), columns=["score", "keyword"])
    keywords_df = keywords_df.loc[keywords_df["score"] > 1]
    keywords_df = keywords_df.loc[~keywords_df["keyword"].isin(COMMON_WORDS)]

    # group matching keywords and sum their scores
    keywords_df = keywords_df.groupby("keyword").sum().reset_index().sort_values(["score"], ascending=False)  # type: ignore

    log.debug(f"keywords_df count {keywords_df.count()}")
    for _, row in keywords_df.iterrows():
        if row["score"] >= 3:
            yield _get_keyword_tuple(row)


def _get_keyword_tuple(row) -> Tuple[str, float]:
    return (row["keyword"], row["score"])


def _format_keyword(keyword) -> str:
    """Return a keyword's main component"""
    return keyword.title()


def _clean(text: str) -> str:
    """Normalise strings by stripping markdown and URLs, lowercasing, and expanding contractions."""
    text = _unmark(text)
    text = re.sub(r"https?://\S+", "", text, flags=re.MULTILINE)
    text = text.lower()
    text = contractions.fix(text)  # type: ignore
    return text


### stuff to remove markdown
# https://stackoverflow.com/questions/761824/python-how-to-convert-markdown-formatted-text-to-text
def _unmark_element(element, stream=None):
    """Clean markdown from comment"""
    if stream is None:
        stream = StringIO()
    if element.text:
        stream.write(element.text)
    for sub in element:
        _unmark_element(sub, stream)
    if element.tail:
        stream.write(element.tail)
    return stream.getvalue()


# patching Markdown
Markdown.output_formats["plain"] = _unmark_element  # type: ignore
__md = Markdown(output_format="plain")  # type: ignore
__md.stripTopLevelTags = False  # type: ignore


def _unmark(text):
    """strip markdown from comment"""
    return __md.convert(text)


if __name__ == "__main__":
    daysago = int(sys.argv[1]) if len(sys.argv) > 1 else None
    process_keywords(daysago=daysago)
