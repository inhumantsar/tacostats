import re

from datetime import datetime, timezone
from io import StringIO

import contractions
import pandas
import nltk

from markdown import Markdown
from pattern.en import parse, parsetree
from pprint import pprint

from tacostats import reddit, io
from tacostats.config import STOPWORDS, COMMON_WORDS, CHUNK_TYPES, BOT_TRIGGERS

def _clean(text):
    text = _unmark(text)
    text = re.sub(r"https?://\S+", "", text, flags=re.MULTILINE)
    text = text.lower()
    text = contractions.fix(text)
    return text

def parse_comment(comment):
    comment = _clean(comment)
    try:
        parsed = parsetree(
            comment,
            tokenize=True,      # Split punctuation marks from words?
            tags=True,          # Parse part-of-speech tags? (NN, JJ, ...)
            chunks=True,        # Parse chunks? (NP, VP, PNP, ...)
            relations=True,     # def: False Parse chunk relations? (-SBJ, -OBJ, ...)
            lemmata=False,      # def: False Parse lemmata? (ate => eat)
            encoding="utf-8",   # Input string encoding.
            tagset=None,
        )
    except Exception as e:
        print(f"\nWARNING: Unable to parse comment: {comment}")
        print(e)
        yield (-1,None)
    else:
        for sentence in parsed:
            for chunk in sentence.chunks:
                score = 0
                score += len(chunk.relations) / 2 if len(chunk.relations) > 0 else 0
                score += 1 if chunk.type in CHUNK_TYPES else 0
                score += 1 if chunk.role is not None else 0
                score += 0.5 if len(chunk.modifiers) > 0 else 0
                score = 0 if chunk.head.string in COMMON_WORDS + STOPWORDS + BOT_TRIGGERS else score
                score = 0 if any([i in chunk.string for i in BOT_TRIGGERS]) else score
                # print(chunk.string, chunk.type, chunk.role, chunk.modifiers, chunk.conjunctions, score)
                if score > 0:
                    # strip crap words
                    s = " ".join(
                        [
                            i
                            for i in chunk.string.split(" ")
                            if (2 < len(i) < 40) and i not in STOPWORDS
                        ]
                    )
                    if s:
                        yield (score, s)


def process_comments(comments):
    cdf = pandas.DataFrame(comments)
    # parse each comment, sorting the results by score
    parsed_comments = cdf[["body"]].apply(
        lambda x: [
            sorted(parse_comment(c), key=lambda y: y[0], reverse=True) for c in x
        ]
    )
    # explode each result (a list of tuples) into rows of tuples
    parsed_comments = parsed_comments.explode("body")

    # create a new dataframe, turning each tuple element into a column and filter common words (why isn't the scoring doing this??)
    keywords_df = pandas.DataFrame(
        parsed_comments["body"].to_list(), columns=["score", "keyword"]
    )
    keywords_df = keywords_df.loc[keywords_df["score"] > 1]
    keywords_df = keywords_df.loc[~keywords_df["keyword"].isin(COMMON_WORDS)]

    # group matching keywords and sum their scores
    keywords_df = (
        keywords_df.groupby("keyword")
        .sum()
        .reset_index()
        .sort_values(["score"], ascending=False)
    )
    return [
        (row["keyword"], row["score"])
        for _, row in keywords_df.iterrows()
        if row["score"] >= 3
    ]

def _format_keyword(keyword):
    return keyword.title()

def process_keywords():
    start = datetime.now(timezone.utc)
    print(f"started at {start}...")

    print(f"downloading stopwords...")
    nltk.data.path.append("/tmp")
    nltk.download('stopwords', download_dir="/tmp")
    print(f"stopwords download complete in {(datetime.now(timezone.utc) - start).total_seconds()} seconds")

    print("getting comments...")
    date, comments = reddit.get_comments()
    s3_prefix = date.strftime("%Y-%m-%d")

    print("processing comments...")
    processed = process_comments(comments)
    filtered = [i[0] for i in processed if i[1] > 3]
    keywords = {
        'keywords_h1': [_format_keyword(i) for i in filtered[:10]],
        'keywords_h2': [_format_keyword(i) for i in filtered[10:30]],
        'keywords_h3': [_format_keyword(i) for i in filtered[30:60]],
        'keywords_h4': [_format_keyword(i) for i in filtered[60:120]],
        'keywords_h5': [_format_keyword(i) for i in filtered[120:180]],
        'keywords_h6': [_format_keyword(i) for i in filtered[180:240]],
    }
    print("writing stats...")
    io.write(s3_prefix, keywords=keywords)

    print("posting comment...")
    reddit.post_comment(keywords, "keywords.md.j2")

    done = datetime.now(timezone.utc)
    duration = (done - start).total_seconds()
    print(f"Finished at {done.isoformat()}, took {duration} seconds")


### stuff to remove markdown
# https://stackoverflow.com/questions/761824/python-how-to-convert-markdown-formatted-text-to-text
def _unmark_element(element, stream=None):
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
Markdown.output_formats["plain"] = _unmark_element
__md = Markdown(output_format="plain")
__md.stripTopLevelTags = False


def _unmark(text):
    """strip markdown from comment"""
    return __md.convert(text)

def lambda_handler(event, context):
    process_keywords()



if __name__ == "__main__":
    process_keywords()
