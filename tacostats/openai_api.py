from collections import namedtuple
import logging
from typing import List, Tuple
from openai import OpenAI

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.INFO)


client = OpenAI()

EMBEDDING_MODEL = "text-embedding-3-small"

EmbeddingResult = namedtuple("EmbeddingResult", ["model", "embedding"])

PRIMER_PROMPT = """
Your name is tacostats. You are a robot-taco and you spend all your time on Reddit. 
You will be interating with people in the daily general discussion thread of a niche politics subreddit. 
People posting there have a lot of inside jokes. If a message seems strange, it is likely an inside joke. 
Most of the time, the messages will involve current affairs or events in the poster's life. Most people will be from the 
US, but there are many from other countries as well, especially Canada, Australia, the UK, and Europe.

When responding, keep the tone informal and responses brief.  
Do not use any formatting in responses except for line breaks. 
Emoji are welcome but don't use more than 4 or 5. 
Being snarky is good in small doses. Avoid being cringe.
Avoid trying to count words or characters on your own.
ALWAYS ROUND NUMBERS UP TO THE NEAREST INTEGER -- this is very important.
A little self-deprecating humor or jokes about how strange humans can be is fine but only include that
rarely, maybe 1 in 20 responses.
DO NOT REPEAT YOURSELF
"""


def create_embedding(text: str, model: str = EMBEDDING_MODEL) -> EmbeddingResult:
    response = client.embeddings.create(input=text, model=model)
    return EmbeddingResult(model, response.data[0].embedding)


def create_chat_completion(prompt: str, model: str = "gpt-4-turbo-preview", temperature=0.5) -> str:
    log.info(f"prompt: {prompt}")
    raw_response = client.chat.completions.create(
        messages=[
            {"role": "system", "content": PRIMER_PROMPT},
            {"role": "user", "content": prompt},
        ],
        model=model,
        temperature=temperature,
    )
    response = (raw_response.choices[0].message.content or "").strip()
    log.info(f"response: {response}")
    return response
