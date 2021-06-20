from datetime import datetime, timedelta, date, time, timezone
from types import SimpleNamespace

import praw

from jinja2 import Environment, PackageLoader, select_autoescape

from tacostats.config import LOCAL_STATS, REDDIT, EXCLUDED_AUTHORS, DRY_RUN, RECAP
from tacostats import io

reddit = praw.Reddit(**REDDIT)

def find_recap_dt(daysago=1):
    posts = reddit.subreddit("neoliberal").search('title:"Discussion Thread" author:jobautomator', sort="new")

    current_utc = datetime.now().astimezone(timezone.utc)
    
    # need to add an extra day if we're between DTs
    if current_utc.hour < 7:
        daysago += 1
    
    target_date = datetime.combine(current_utc.date(), time(hour=7, tzinfo=timezone.utc)) - timedelta(days=daysago)
    print('looking for old dt, target: ', target_date)

    for post in posts:
        post_dt = datetime.utcfromtimestamp(post.created_utc)
        # print(f'checking {post} {post_dt} {(post_dt.year, post_dt.month, post_dt.day)} {(target_date.year, target_date.month, target_date.day)}')
        if (post_dt.year, post_dt.month, post_dt.day) == (target_date.year, target_date.month, target_date.day):
            print(f'found it: {post} {post_dt}')
            return post


def find_dt():
    dt = reddit.subreddit("neoliberal").sticky()
    if dt.title == "Discussion Thread":
        return dt
    # first sticky might be something else, try second
    dt = reddit.subreddit("neoliberal").sticky(number=2)
    if dt.title == "Discussion Thread":
        return dt

    raise('No DT Found!')

def _actually_post_comment(dt, comment_body):
    if DRY_RUN:
        print(comment_body)
        print(f"\nThe above comment would have been written to {datetime.utcfromtimestamp(dt.created_utc)}")
    else:
        dt.reply(comment_body) 


def post_comment(data, template_name):
    jinja_env = Environment(
        loader=PackageLoader("tacostats"), autoescape=select_autoescape()
    )
    template = jinja_env.get_template(template_name)

    # recap dt will be today-1d. post to it as if it is still today
    if RECAP:
        recap_dt = find_recap_dt()
        print(recap_dt)
        _actually_post_comment(recap_dt, template.render(YESTER=False, **data))
    
    # post to the dt provided or else to today's dt
    todays_dt = find_dt()
    _actually_post_comment(todays_dt, template.render(YESTER=RECAP, **data))


def get_comments(force_cache=False, dt=None):
    start = datetime.now(timezone.utc)
    print(f"starting get comments at {start}...")
    comments_list = None
    local = False
    # read local comment cache if it's recent
    if io.cache_available() or force_cache:
        cache = io.read_cache()
        comments_list = [SimpleNamespace(**i) for i in cache["comments"]]
        dt_date = datetime.utcfromtimestamp(cache["date"])
        local = True
    else:
        dt = find_dt() if not RECAP else find_recap_dt()
        dt_date = datetime.utcfromtimestamp(dt.created_utc)
        print(f"Got dt {dt.id} created at {dt_date.isoformat()}")

        dt.comment_sort = "new"
        dt.comment_limit = 1000

        print("\nRunning replace_more... This will take a while...")
        dt.comments.replace_more(limit=None)
        print(
            f"MoreComments replacement complete in {(datetime.now(timezone.utc) - start).total_seconds()} seconds"
        )
        comments_list = dt.comments.list()

    comments = []
    other = 0
    # for c in iter_top_level(dt.comments):
    for c in comments_list:
        # recaps end up coming with a handful of comments from the next day
        c_date = datetime.utcfromtimestamp(c.created_utc)
        if c_date > dt_date + timedelta(days=1):
            print(f'comment too new: {c} {c_date}')
            continue
        if not c.author:
            comments.append(
                {
                    "author": "",
                    "author_flair_text": "",
                    "score": 0,
                    "id": c.id,
                    "permalink": c.permalink,
                    "body": c.body,
                    "created_utc": c.created_utc,
                }
            )
            continue

        author = c.author if isinstance(c.author, str) else c.author.name
        if author in EXCLUDED_AUTHORS:
            continue

        try:
            comments.append(
                {
                    "author": author,
                    "author_flair_text": c.author_flair_text,
                    "score": c.score,
                    "id": c.id,
                    "permalink": c.permalink,
                    "body": c.body,
                    "created_utc": c.created_utc,
                }
            )
        except Exception as e:
            print(f"{c.id}: {e}")

    # don't re-write the cache we just read from...
    if LOCAL_STATS and not local:
        print("writing local comments cache...")
        io.write_cache({"comments": comments, "date": dt_date.timestamp()})

    d = (datetime.now(timezone.utc) - start).total_seconds()
    print(f"done! duration: {d} // len: {len(comments)} // o: {other}")
    return dt_date, comments
