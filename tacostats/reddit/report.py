from datetime import datetime
from typing import Union

from jinja2 import Template, Environment, PackageLoader, select_autoescape
from praw.models.reddit.submission import Submission
from praw.reddit import Comment

from tacostats.reddit.dt import DT, get_comment, recap, current
from tacostats.config import DRY_RUN, RECAP

def reply(data: dict, template_name: str, comment_id: str):
    """Replies to another comment using template and data supplied."""
    body = _render_template(data, template_name)
    _actually_post(get_comment(comment_id), body)


def post(data: dict, template_name: str):
    """Posts comment using template and data supplied. Injects `YESTER` for recap comments."""
    # when recapping, post to original dt as well as today's
    if RECAP:
        recap_dt = recap()
        template_data = {"YESTER": False, **data}
        body = _render_template(template_data, template_name)
        _actually_post(recap_dt.submission, body)

    todays_dt = current()
    template_data = {"YESTER": RECAP, **data}
    body = _render_template(template_data, template_name)
    _actually_post(todays_dt.submission, body)


def _actually_post(target: Union[Comment, Submission], body: str):
    """Posts comment to DT or prints it to screen"""
    if DRY_RUN:
        print(f"\n{body}")
        print(
            f"\n---------------\n--- The above comment would have been written to {target.id}\n---------------"
        )
    else:
        target.reply(body)
        print(f"comment posted to {target.id}")


def _render_template(data: dict, template_name: str) -> str:
    """Reads Jinja template in and fills it with `data`, returning the rendered body"""
    jinja_env = Environment(
        loader=PackageLoader("tacostats"), autoescape=select_autoescape()
    )
    template = jinja_env.get_template(template_name)
    print(f"got template: {template_name} {template}, rendering...")
    return template.render(**data)
