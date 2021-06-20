import re
import regex  # supports grapheme search

from datetime import datetime, timezone

import emoji
import pandas
import pytz

from tacostats import reddit, io

NEUTER_RE = re.compile(r"!ping", re.MULTILINE | re.IGNORECASE | re.UNICODE)

def _neuter_ping(comment):
    comment["body"] = NEUTER_RE.sub("*ping", comment["body"])
    return comment


def _find_activity_by_hour(tdf):
    activity = (
        tdf[["word_count"]].groupby(pandas.Grouper(freq="H")).agg(["sum", "count"])
    )
    maximums = activity.max()["word_count"]
    minimums = activity.min()["word_count"]
    activity["norm_count"] = (activity["word_count"]["count"] - minimums["count"]) / (
        maximums["count"] - minimums["count"]
    )
    return list(activity["norm_count"].to_dict().values())


def _find_wordiest_by_hour(tdf):
    redditor_activity = (
        tdf[["word_count", "author"]]
        .groupby([pandas.Grouper(freq="H"), "author"])
        .sum()
    )
    redditor_activity = (
        redditor_activity[
            redditor_activity == redditor_activity.groupby(level=0).transform("max")
        ]
        .dropna()
        .reset_index()
    )
    redditor_activity["created_et"] = (
        redditor_activity["created_et"].apply(lambda x: x.value / 10 ** 9).astype(int)
    )
    redditor_activity.to_dict("records")
    return redditor_activity.to_dict("records")


def _find_spammiest_by_hour(tdf):
    spammiest_s = tdf.groupby([pandas.Grouper(freq="H"), "author"]).size()
    d = {}
    for k, v in spammiest_s.iteritems():
        dt = int(k[0].value / 10**9)
        if dt not in d.keys() or d[dt][1] < v:
            d[dt] = (k[1], v)
    return [{'created_et': k, 'author': v[0], 'comment_count': v[1]} for k, v in d.items()]


def _find_emoji_spammers(cdf):
    return (
        cdf[["author", "emoji_count"]]
        .groupby("author")
        .sum()
        .reset_index()
        .sort_values(by="emoji_count", ascending=False)
        .to_dict("records")
    )


def _find_top_emoji(cdf):
    top_emoji = cdf['body'].apply(_find_emoji)
    top_emoji = top_emoji.where(top_emoji.str.len() > 0).dropna().explode().value_counts()
    return list(zip(top_emoji, top_emoji.index))


def _find_emoji(body):
    emojis = emoji.UNICODE_EMOJI['en'].keys()
    # \X matches graphemes, ie: regular chars as well as combined chars like letter+ligature or 👨‍👩‍👦‍👦
    matches = regex.findall(r'\X', body)
    return [i for i in matches if i in emojis]

def _count_emoji(body):
    return len(_find_emoji(body))


def process_comments(comment_list, short_stats_max=3):
    cdf = pandas.DataFrame(comment_list)

    # count deleted and removed
    deleted = int(cdf.loc[cdf['body'] == '[deleted]'].size)
    removed = int(cdf.loc[cdf['body'] == '[removed]'].size)
    # find other blank author fields
    other_blank = int(cdf.loc[cdf['author'] == ''].size)

    # remove blank author fields
    cdf = cdf[cdf['author'] != '']

    # add derived columns
    cdf["emoji_count"] = cdf["body"].apply(_count_emoji)

    # Memeiest
    memeiest_terms = ["👆", "👉", "👇", "👈", "☝️"]
    memeiest_df = (
        cdf[cdf["body"].str.contains("|".join(memeiest_terms))][["author"]]
        .value_counts()
        .reset_index()
        .rename(columns={0: "meme_count"})
        .sort_values(["meme_count"], ascending=False)
    )
    memeiest_full = memeiest_df.to_dict("records")

    # Spammiest Users
    spammiest_df = (
        cdf[["author", "author_flair_text"]]
        .value_counts()
        .reset_index()
        .rename(columns={0: "comment_count"})
        .sort_values(["comment_count"], ascending=False)
    )
    spammiest_full = spammiest_df.to_dict("records")

    # Most Upvoted Comments
    upvoted_comments_full = cdf.sort_values(["score"], ascending=False).to_dict(
        "records"
    )

    # Most Upvoted Redditors
    upvoted_redditors_df = (
        cdf[["author", "score"]]
        .groupby("author")
        .sum()
        .reset_index()
        .sort_values(["score"], ascending=False)
    )
    upvoted_redditors_full = upvoted_redditors_df.to_dict("records")

    avg_score_df = pandas.merge(upvoted_redditors_df, spammiest_df, on="author")
    avg_score_df["avg_score"] = (
        avg_score_df["score"] / avg_score_df["comment_count"]
    ).round(decimals=1)

    avg_score_full = (
        avg_score_df[["author", "avg_score"]]
        .sort_values("avg_score", ascending=False)
        .to_dict("records")
    )

    # Wordiest Redditors
    cdf["word_count"] = cdf["body"].str.count(" ") + 1
    wordiest_df = (
        cdf[["author", "author_flair_text", "word_count"]]
        .groupby("author")
        .sum()
        .reset_index()
        .sort_values(["word_count"], ascending=False)
    )
    wordiest_full = wordiest_df.to_dict("records")

    wordiest_per_df = pandas.merge(wordiest_df, spammiest_df, on="author")
    wordiest_per_df["avg_words"] = (
        wordiest_per_df["word_count"] / wordiest_per_df["comment_count"]
    ).round(decimals=1)
    wordiest_per_full = (
        wordiest_per_df[["author", "author_flair_text", "avg_words"]]
        .sort_values("avg_words", ascending=False)
        .to_dict("records")
    )

    # Flair Population
    # flairpop_df = cdf[['author', 'author_flair_text']].groupby('author_flair_text').size().sort_values(ascending=False)
    # flairpop_full = flairpop_df.to_dict('records')

    ### Time-based
    # prep timeseries index relative to
    tdf = cdf.copy(deep=True)
    tdf.head(1)
    tdf["created_utc"] = tdf["created_utc"].apply(
        lambda x: datetime.fromtimestamp(x, tz=timezone.utc).astimezone(
            pytz.timezone("US/Eastern")
        )
    )
    tdf.rename(columns={"created_utc": "created_et"}, inplace=True)
    tdf.set_index("created_et", inplace=True)

    # Activity Sparklines - These don't align well and aren't super useful as a result...
    # chars = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█']
    # activity = tdf[['word_count']].groupby(pandas.Grouper(freq='H')).agg(['sum', 'count'])
    # maximums = activity.max()['word_count']
    # count_buckets = dict(zip([1] + [maximums['count']*((i+1)/7) for i in range(7)], chars))
    # # word_buckets = dict(zip([1] + [maximums['sum']*((i+1)/7) for i in range(7)], chars))

    # def get_char(value, buckets):
    #     match = ' '
    #     for k, v in buckets.items():
    #         if value >= k:
    #             match = v
    #     return match

    # activity['count_chars'] = [get_char(x, count_buckets) for x in activity['word_count']['count'].values]
    # comment_count_sparkline = ''.join(activity['count_chars'])

    # build stats dicts
    full_stats = {
        "deleted": deleted,
        "removed": removed,
        "other_blank": other_blank,
        "spammiest": spammiest_full,
        "wordiest_overall": wordiest_full,
        "wordiest": wordiest_per_full,
        "upvoted_comments": upvoted_comments_full,
        "upvoted_redditors": upvoted_redditors_full,
        "best_redditors": avg_score_full,
        "memeiest": memeiest_full,
        # 'sparklines': {'comment_count': comment_count_sparkline},
        "activity": _find_activity_by_hour(tdf),
        "hourly_wordiest": _find_wordiest_by_hour(tdf),
        "hourly_spammiest": _find_spammiest_by_hour(tdf),
        "emoji_spammers": _find_emoji_spammers(cdf),
        "top_emoji": _find_top_emoji(cdf),
    }

    short_stats = {
        "deleted": deleted,
        "removed": removed,
        "other_blank": other_blank,
        "spammiest": spammiest_full[:short_stats_max],
        "wordiest_overall": wordiest_full[:short_stats_max],
        "wordiest": wordiest_per_full[:short_stats_max],
        "upvoted_comments": upvoted_comments_full[:short_stats_max],
        "upvoted_redditors": upvoted_redditors_full[:short_stats_max],
        "best_redditors": avg_score_full[:short_stats_max],
        "memeiest": memeiest_full[:short_stats_max],
        # 'sparklines': {'comment_count': comment_count_sparkline},
        "activity": full_stats["activity"],
        "hourly_wordiest": full_stats["hourly_wordiest"],
        "hourly_spammiest": full_stats["hourly_spammiest"],
        "emoji_spammers": full_stats["emoji_spammers"],
        "top_emoji": full_stats["top_emoji"],
    }

    return full_stats, short_stats


def process_stats():
    start = datetime.now(timezone.utc)
    print(f"started at {start}...")

    dt = reddit.find_dt()
    date, comments = reddit.get_comments(dt=dt)
    s3_prefix = date.strftime("%Y-%m-%d")

    print("processing.comments...")
    full_stats, short_stats = process_comments(comments)

    print("writing stats...")
    io.write(
        s3_prefix,
        comments=comments,
        full_stats=full_stats,
        short_stats=short_stats,
    )

    # neuter upvoted comments to prevent pinging groupbot
    short_stats["upvoted_comments"] = [
        _neuter_ping(c) for c in short_stats["upvoted_comments"]
    ]

    template = "template.md.j2"
    print(f"posting comment using {template}...")
    reddit.post_comment(short_stats, template)

    done = datetime.now(timezone.utc)
    duration = (done - start).total_seconds()
    print(f"Finished at {done.isoformat()}, took {duration} seconds")

def lambda_handler(event, context):
    process_stats()

if __name__ == "__main__":
    process_stats()
