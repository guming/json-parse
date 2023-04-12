from typing import Any

from .instance import default_instance


def query(query: str,data: Any) -> Any:
    return default_instance.query(query, data);