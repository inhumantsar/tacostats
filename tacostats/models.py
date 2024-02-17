from dataclasses import asdict, dataclass, field
from datetime import datetime
import json
from re import L
from typing import Any, Dict, List, Optional, Tuple


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
    parent_id: Optional[str] = None

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

    def to_slim_dict(self) -> Dict[str, Any]:
        return {"author": self.author, "body": self.body, "score": self.score}

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
                ("parent_id", "TEXT"),
                ("permalink", "TEXT"),
                ("body", "TEXT"),
                ("created_utc", "TIMESTAMP"),
                ("embedding", "vector"),
                ("embedding_model", "TEXT"),
            ],
        )


@dataclass
class Thread:
    comment: Comment
    parent: Optional["Thread"] = None
    children: List["Thread"] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "comment": self.comment.to_dict(),
            "parent": self.parent.comment.id if self.parent else None,
            "children": [c.to_dict() for c in self.children],
        }

    def to_slim_text(self, layer=0) -> str:
        thread_str = "" if layer else f"--- {self.comment.created_utc.isoformat(timespec='minutes')} UTC ---\n"
        thread_str += " " * layer + f"{self.comment.author} ({self.comment.score}):"
        thread_str += "\n".join(" " * layer + line for line in self.comment.body.splitlines()) + "\n"
        for child in self.children:
            thread_str += child.to_slim_text(layer + 1)
        return thread_str + "\n--- thread ends ---\n"

    def to_slim_dict(self) -> Dict[str, Any]:
        d: Dict[str, str | List[Dict] | Dict] = {"comment": self.comment.to_slim_dict()}
        if self.parent:
            d["parent"] = self.parent.comment.id
        if self.children:
            d["children"] = [c.to_slim_dict() for c in self.children]
        return d

    def contains(self, comment: Comment) -> bool:
        if self.comment.id == comment.id:
            return True
        for child in self.children:
            if child.contains(comment):
                return True
        return False

    def get_comment_ids(self) -> List[str]:
        ids = [self.comment.id]
        for child in self.children:
            ids.extend(child.get_comment_ids())
        return ids

    def get_size(self) -> int:
        """Returns the total number of characters in the thread."""
        chars: int = len(self.comment.body)
        for child in self.children:
            chars += child.get_size()
        return chars

    def get_avg_score(self) -> int:
        score: int = self.comment.score
        for child in self.children:
            score += child.get_avg_score()
        return score // (len(self.children) + 1)

    def get_score_minmax(self) -> Tuple[int, int]:
        """Returns the minimum and maximum scores of the thread."""
        overall_min = self.comment.score
        overall_max = self.comment.score
        for child in self.children:
            child_min, child_max = child.get_score_minmax()
            overall_min = min(overall_min, child_min)
            overall_max = max(overall_max, child_max)
        return overall_min, overall_max
