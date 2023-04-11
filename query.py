from typing import Any

from instance import instance


def query(query: str,data: Any) -> Any:
    return instance.query(query, data);