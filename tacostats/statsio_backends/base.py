from typing import Any, Dict, List


class BaseBackend:
    """Base class for StatsIO backends"""

    @staticmethod
    def write(prefix: str, **kwargs):
        """write local stats files. use kwargs keys for name, values for data"""
        pass

    @staticmethod
    def read(prefix: str, key: str) -> Any:
        """read local stats file"""
        pass

    @staticmethod
    def read_comments(prefix: str) -> List[Dict[str, Any]]:  # type: ignore
        """read local comments file"""
        pass

    @staticmethod
    def get_listing() -> List[str]:  # type: ignore
        pass

    @staticmethod
    def get_age(prefix: str, key: str) -> int:  # type: ignore
        """get number of seconds since object was last modified"""
        pass
