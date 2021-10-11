#
# monthly stats
#
# wishlist:
#   - top posters
#   - top emoji
#   - comments fashed
#   - top keywords
#   - % change for all above vs prev interval
#   - thunderdome indicators
#   - activity heatmap w/ overall comment counts

import logging

from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple

import cairo

from pandas import DataFrame

from tacostats import statsio
from tacostats.config import KEYWORDS_KEY, FULLSTATS_KEY, THUNDERDOME_TITLES, get_storage_prefix


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
logging.getLogger("praw").setLevel(logging.WARNING)
logging.getLogger("prawcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

_stats = {}
_dfs = {}

def save_records(data):
    pass

def get_records(day):
    pass

def get_stats(day: date) -> Dict:
    """Fetch full stats from storage for a specified day. Keep in memory."""
    storage_prefix = get_storage_prefix(day)
    if storage_prefix not in _stats.keys():
        _stats[storage_prefix] = statsio.read(storage_prefix, FULLSTATS_KEY)
    
    return _stats[storage_prefix]

def get_keywords(day: date) -> Dict:
    """Fetch keywords from storage for a specified day. Keep in memory."""
    storage_prefix = get_storage_prefix(day)
    if storage_prefix not in _stats.keys():
        _stats[storage_prefix] = statsio.read(storage_prefix, KEYWORDS_KEY)
    
    return _stats[storage_prefix]

def get_dataframe(day: date, key: str) -> DataFrame:
    """Build dataframe from full stats from storage for a specified day. Keep in memory."""
    if day not in _dfs.keys() or key not in _dfs[day].keys():
        _dfs[day][key] = DataFrame(get_stats(day).get(key))
    
    return _dfs[day][key]


# -----------------------------------------------------------------------------------------------------------------------------------------
# |                |                |                |                |    ⚡⚡⚡     |                |                |                |
# |  10k comments  |  10k comments  |  10k comments  |  10k comments  |  10k comments  |  10k comments  |  10k comments  |  10k comments  | 
# |  +15% vs avg   |  +15% vs avg   |  +15% vs avg   |  +15% vs avg   |  +15% vs avg   |  +15% vs avg   |  +15% vs avg   |  +15% vs avg   | 
# |  456 fashed    |  456 fashed    |  456 fashed    |  456 fashed    |  456 fashed    |  456 fashed    |  456 fashed    |  456 fashed    | 
# |    "Worms"     |   "Sinema"     |   "Biden"      |    "Trump"     |    "Trump"     |    "Worms"     |    "Pasta"     |     "Trump"    |
# |  🤣😁😍😐🤝 |  🤣😁😍😐🤝  |  🤣😁😍😐🤝 |  🤣😁😍😐🤝  |  🤣😁😍😐🤝 |  🤣😁😍😐🤝  |  🤣😁😍😐🤝 |  🤣😁😍😐🤝  |
# |                |                |                |                |                |                |                |                | 
# -----------------------------------------------------------------------------------------------------------------------------------------

def get_daily_record(day):
    


def get_comment_count(day):
    log.debug(f"getting comment count for {day}")
    count = len(get_stats(day)['upvoted_comments'])
    return round(count / 1000, 1)

def get_comment_change_percent(day, daysago):
    log.debug(f"comparing comment count from {day} to the count from {daysago} days ago.")
    target_day = day - timedelta(days=daysago)
    cur = get_comment_count(day)
    prev = get_comment_count(target_day)
    return round(((cur - prev) / prev) * 100, 0)
    
def get_fashed_count(day):
    log.debug(f"getting fashed comment count for {day}")
    return get_keywords(day)['removed']

def get_top_keyword(day: date) -> str:
    log.debug(f"getting top keyword for {day}")
    return get_keywords(day)['keyword_scores'][0][0]

def get_top_emoji(day: date, count: int=5) -> List[List]:
    """Gets top `count` emoji for `day`.
    
    Returns: [[score, emoji], ...]
    """
    log.debug(f"getting top {count} emoji for {day}")
    return get_stats(day)['top_emoji'][:5]

def is_thunderdome_day(day):
    df = get_dataframe(day, "upvoted_comments")
    log.debug(f'is_thunderdome_day: got {len(df)} comments')
    s = df["permalink"].apply(lambda x: x[:-9]).drop_duplicates()
    log.debug(f'is_thunderdome_day: got {s.count()} comments after dedupe')
    for i in s:
        log.debug(f'is_thunderdome_day: checking {i}')
        if any([t.replace(' ', '_') in i for t in THUNDERDOME_TITLES]):
            return True
    log.debug('is_thunderdome_day: did not find a thunderdome title.')
    return False

# -----------------------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------
# 24      12       24      12       24      12       24      12       24      12       24      12       24      12       24      12       24
# 1   2   3   4   5   6   7   8   9   10   11   12   13   14   15   16   17   18   19   20   21   22   23   24   25   26   27   28   29   30
# ...maybe... # Euro Hours Average: 12 cph  --  Burger Hours Average: 30 cph  --  Asian Hours Average: 8 cph

def build_rate_graph(date_range, period):
    pass

def get_comments_per_hour(day):
    pass

# def get_comment_count(day):
#     pass

# 0. RecordHlder: |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||| (28765 comments, all time record)
# 1. SaltySalad:  |||||||||||||||||||||||||||||||||||||||||||| (15123 comments, n/c vs last week)
# 2. SOmeoneElse: |||||||||||||||||||||||||||||| (9876 comments, +3)
# 3. ANother:     |||||||||||||||||||||||||||| (9765 comments, -2 vs last week)
# 4. Always4th:   ||||||||||||||||||||| (8765 comments, n/c vs last week)
# 5. iloveoof:    |||||||||||||| (6543 comments, +1 vs last week)

def build_leaderboard_chart(date_range, period):
    pass

def get_top_posters(period, count=5):
    pass

# wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud 
# wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud
# wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud 
# wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud
# wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud 
# wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud
# wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud 
# wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud wordcloud

def build_word_cloud(date_range):
    pass

def get_top_keywords(date_range):
    pass