import json
import logging

from collections import namedtuple
from typing import Dict, List, Optional, Tuple

from openai import OpenAI
import tiktoken

from tacostats.config import CHAT_MODEL, EMBEDDING_MODEL, PRIMER_PROMPT, DEFAULT_TEMPERATURE, RULES_PROMPT, MAX_TOKENS

log = logging.getLogger(__name__)


client = OpenAI()

EmbeddingResult = namedtuple("EmbeddingResult", ["model", "embedding"])


class MaxTokensExceededError(Exception):
    current_tokens: int
    max_tokens: int

    def __init__(self, message: str, current_threads: int, max_threads: int):
        super().__init__(message)
        self.current_tokens = current_threads
        self.max_tokens = max_threads


def estimate_tokens(text: str, model) -> int:
    encoding = tiktoken.encoding_for_model(model)
    return len(encoding.encode(text))


def build_prompts(
    chat_prompt: Optional[str] = None,
    system_prompt: Optional[str] = None,
    model: str = CHAT_MODEL,
) -> Tuple[List[Dict[str, str]], int]:
    """Returns chat completion prompts and the estimated token count."""

    prompts = [
        {"role": "system", "content": PRIMER_PROMPT},
        {"role": "system", "content": system_prompt or ""},
        {"role": "system", "content": RULES_PROMPT},
        {"role": "user", "content": chat_prompt or ""},
    ]

    return prompts, estimate_tokens(json.dumps(prompts), model)


def create_embedding(text: str, model: str = EMBEDDING_MODEL) -> EmbeddingResult:
    response = client.embeddings.create(input=text, model=model)
    return EmbeddingResult(model, response.data[0].embedding)


def create_chat_completion(
    chat_prompt: str,
    system_prompt: Optional[str] = None,
    model: str = CHAT_MODEL,
    temperature=DEFAULT_TEMPERATURE,
) -> str:
    prompts, size = build_prompts(chat_prompt, system_prompt)
    log.info(f"prompts token count: {size}")
    log.debug(f"prompt data: {json.dumps(prompts, indent=2)}")

    if size > MAX_TOKENS:
        raise MaxTokensExceededError(
            f"chat prompt token count {size} exceeds the maximum token count {MAX_TOKENS}",
            current_threads=size,
            max_threads=MAX_TOKENS,
        )

    raw_response = client.chat.completions.create(
        messages=prompts,  # type: ignore
        model=model,
        temperature=temperature,
    )
    response = (raw_response.choices[0].message.content or "").strip()
    log.info(f'chat completion response\n"""\n{response}\n"""')
    return response
