from enum import Enum
from typing import Union, Any, List, Dict

from typeguard import typechecked

from runtime_value import RuntimeValue


class ExpressionType(Enum):
    Fncall = "fncall",
    Reference = "reference"
    Literal = "literal"
    Value = "value"
    Array = "array"
    Object = "object"
    Pipe = "pipe"


class BaseExpression:
    @typechecked
    def __init__(self, type: ExpressionType):
        self.type = type

class FnExpression(BaseExpression):
    @typechecked
    def __init__(self, fn: BaseExpression, args: List[BaseExpression]):
        super().__init__(ExpressionType.Fncall)
        self.fn = fn
        self.args = args

class RefExpression(BaseExpression):
    @typechecked
    def __init__(self, name: str, absolute: bool = False):
        super().__init__(ExpressionType.Reference)
        self.name = name
        self.absolute = absolute

class ArrayExpression(BaseExpression):
    @typechecked
    def __init__(self, items: List[BaseExpression]):
        super().__init__(ExpressionType.Array)
        self.items = items


class ObjectExpression(BaseExpression):
    @typechecked
    def __init__(self, entries: Dict[str, BaseExpression]):
        super().__init__(ExpressionType.Object)
        self.entries = entries


class PipeExpression(BaseExpression):
    @typechecked
    def __init__(self, stages: List[BaseExpression]):
        super().__init__(ExpressionType.Pipe)
        self.stages = stages

class ValueExpression(BaseExpression):
    @typechecked
    def __init__(self, value: RuntimeValue):
        super().__init__(ExpressionType.Value)
        self.value = value

    @classmethod
    def of(cls, value: Any):
        return cls(RuntimeValue.of(value))


Expression = Union[
    FnExpression,
    RefExpression,
    ValueExpression,
    ArrayExpression,
    ObjectExpression,
    PipeExpression,
]