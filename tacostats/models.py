from dataclasses import asdict, dataclass, field
from datetime import datetime
import json
from re import L
from typing import Any, Dict, List, Optional


@dataclass
class Comment:
    author: str
    author_flair_text: Optional[str]
    score: int
    id: str
    permalink: str
    body: str
    created_utc: datetime
    embedding_model: Optional[str] = None
    embedding: Optional[List[float]] = field(default_factory=list)

    @staticmethod
    def from_json(json_str: str) -> "Comment":
        data = json.loads(json_str)
        return Comment.from_dict(data)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Comment":
        data["created_utc"] = datetime.fromtimestamp(data["created_utc"])
        return Comment(**data)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_prompt_string(self) -> str:
        return f"At {self.created_utc.isoformat()}, {self.author} ({self.author_flair_text}) wrote:\n{self.body}"

    def to_embedding_string(self) -> str:
        return f"@{self.author} ({self.author_flair_text})\n{self.body}"

    @staticmethod
    def get_table_info():
        return (
            "comments",
            [
                ("author", "TEXT"),
                ("author_flair_text", "TEXT"),
                ("score", "INTEGER"),
                ("id", "TEXT", "PRIMARY KEY"),
                ("permalink", "TEXT"),
                ("body", "TEXT"),
                ("created_utc", "TIMESTAMP"),
                ("embedding", "vector"),
                ("embedding_model", "TEXT"),
            ],
        )
