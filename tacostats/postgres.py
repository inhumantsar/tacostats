from datetime import datetime
from itertools import chain
import logging
from re import M
from typing import Any, List, Optional, Tuple

import psycopg2
from psycopg2.extensions import connection

from tacostats.models import Comment


TABLES = [Comment.get_table_info()]

COLUMNS = {k: [col[0] for col in v] for k, v in TABLES}

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
logging.getLogger("psycopg2").setLevel(logging.WARNING)


def initialize_connection(host: str, port: int, database: str, user: str, password: str) -> Optional[connection]:
    try:
        conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None


def close_connection(conn: connection) -> None:
    conn.close()


def execute_query(conn: connection, query: str, values: Tuple[Any] | None = None) -> Optional[List[Tuple[Any, ...]]]:
    try:
        cursor = conn.cursor()
        cursor.execute(query, values)
        result = cursor.fetchall()
        cursor.close()
        return result
    except psycopg2.Error as e:
        print(f"Error executing query: {e}")
        return None


def execute_write(conn: connection, query: str, values: Tuple[Any] | None = None) -> None:
    try:
        cursor = conn.cursor()
        cursor.execute(query, values)
        conn.commit()
        cursor.close()
    except psycopg2.Error as e:
        print(f"Error executing query: {e}")


def initialize_extensions(conn: connection) -> None:
    execute_write(conn, "CREATE EXTENSION IF NOT EXISTS vector;")


def initialize_tables(conn: connection) -> None:
    query = ""
    for table_name, columns in TABLES:
        col_query = ", ".join([" ".join(list(col)) for col in columns])
        query += f"CREATE TABLE IF NOT EXISTS {table_name} ({col_query});"

    log.info("Creating tables:" + "\n  - ".join(query.split("CREATE TABLE IF NOT EXISTS ")))
    execute_write(conn, query)


def insert_comment(conn: connection, comment: Comment, noop: bool = False) -> Tuple[str, Tuple[Any, ...]]:
    values = []
    cols = []

    for name, value in comment.to_dict().items():
        if not value or name not in COLUMNS["comments"]:
            continue

        cols.append(name)
        values.append(value)

    query = f"INSERT INTO comments ({','.join(cols)}) VALUES ({'%s, ' * (len(values) - 1)} %s) ON CONFLICT DO NOTHING; "

    if not noop:
        execute_write(conn, query, tuple(values))
    return query, tuple(values)


def bootstrap(conn: connection, comments: List[Comment]) -> None:
    initialize_extensions(conn)
    initialize_tables(conn)

    megainsert_query = ""
    megainsert_values = []
    for comment in comments:
        insert_query, insert_values = insert_comment(conn, comment, noop=True)
        megainsert_query += insert_query
        megainsert_values += insert_values
    log.info(f"Inserting {len(comments)} comments...")
    execute_write(conn, megainsert_query, tuple(megainsert_values))
