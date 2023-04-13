from typing import Dict, Union, Callable

from .convert import convert_to_runtime, convert_to_python
from .execute import execute, execute_outer
from .json_query_parse import parse
from .runtime_value import RuntimeValue


class JsonQLInstance:
    def __init__(self, extras: Dict[str, Union[RuntimeValue, Callable]] = None):
        self.extras = extras or {}

    def query(self, query, data):
        ast = parse(query)
        data = convert_to_runtime(data)
        # print('ast',ast.__dict__)
        print(data)
        result = execute_outer(ast, data, self.extras)
        return_value = convert_to_python(result)
        return return_value

default_instance = JsonQLInstance();