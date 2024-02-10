from datetime import date, timedelta
import os
import time
from tacostats.postgres import initialize_connection, close_connection, bootstrap
from tacostats.models import Comment
from tacostats.statsio import read_comments
from tacostats.openai_api import create_embedding


def process_comment(comment: dict) -> Comment:
    c = Comment.from_dict(comment)
    # embedding = create_embedding(c.to_embedding_string())
    # c.embedding_model = embedding.model
    # c.embedding = embedding.embedding
    return c


def main():
    connection = initialize_connection(
        database=os.getenv("POSTGRES_DB", "tacostats"),
        user=os.getenv("POSTGRES_USER", "tacostats"),
        password=os.getenv("POSTGRES_PASS", "tacostats"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
    )

    if not connection:
        return

    dt = date.today() - timedelta(days=1)  # TODO: read from env var
    comments = [process_comment(c) for c in read_comments(dt)]
    bootstrap(connection, comments)

    close_connection(connection)


if __name__ == "__main__":
    main()
