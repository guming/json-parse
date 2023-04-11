import datetime
import json
import time
from datetime import date
from enum import Enum
import re
from math import isfinite, isnan
from typing import Any, Callable, Dict, Set, Union, Optional
import inspect
from exception import JsonQLTypeError, OpenAnIssueIfYouGetThisError

'''
copy from mistql
'''
class RuntimeValueType(Enum):
    Null = "null"
    Boolean = "boolean"
    Number = "number"
    String = "string"
    Object = "object"
    Array = "array"
    Function = "function"
    Regex = "regex"

UPPER_NUM_FORMATTING_BREAKPOINT = 1e21
LOWER_NUM_FORMATTING_BREAKPOINT = 1e-7
MAX_SAFE_INT = 2 ** 53 - 1


e_zero_regex = re.compile(r"e-0+")


def format_number(value: float) -> str:
    if value < UPPER_NUM_FORMATTING_BREAKPOINT and value >= MAX_SAFE_INT:
        return str(int(value))
    elif value < UPPER_NUM_FORMATTING_BREAKPOINT and value == int(value):
        return str(int(value))
    elif value < 1 and value <= LOWER_NUM_FORMATTING_BREAKPOINT:
        formatted = str(value)
        return e_zero_regex.sub("e-", formatted)
    elif value < 1:
        formatted = "{:.16f}".format(value)
        formatted = formatted.rstrip("0")
        return str(formatted)
    else:
        return json.dumps(value)


class RuntimeValue:
    @staticmethod
    def of(value):
        """
        Convert a Python value into a MistQL RuntimeValue
        """
        if isinstance(value, RuntimeValue):
            return value
        elif value is None:
            return RuntimeValue(RuntimeValueType.Null)
        elif isinstance(value, bool):
            return RuntimeValue(RuntimeValueType.Boolean, value)
        elif isinstance(value, int):
            return RuntimeValue(RuntimeValueType.Number, float(value))
        elif isinstance(value, float):
            if isnan(value) or not isfinite(value):
                return RuntimeValue(RuntimeValueType.Null)
            return RuntimeValue(RuntimeValueType.Number, value)
        elif isinstance(value, str):
            return RuntimeValue(RuntimeValueType.String, value)
        elif isinstance(value, list) or isinstance(value, tuple):
            return RuntimeValue(
                RuntimeValueType.Array,
                [RuntimeValue.of(item) for item in value],
            )
        elif isinstance(value, dict):
            return RuntimeValue(
                RuntimeValueType.Object,
                {key: RuntimeValue.of(value[key]) for key in value},
            )
        elif (isinstance(value, date) or
                isinstance(value, datetime) or
                isinstance(value, time)):
            return RuntimeValue(RuntimeValueType.String, value.isoformat())
        else:
            raise ValueError(
                "Cannot convert external type to MistQL type: " + str(type(value))
            )

    @staticmethod
    def wrap_function_def(definition: Callable):
        """
        Create a new function that can be used in MistQL expressions.
        """
        return RuntimeValue(
            RuntimeValueType.Function,
            definition,
        )

    @staticmethod
    def from_py_func(py_func: Callable):
        """
        Create a new function from a Python function that can be used in MistQL
        """
        spec = inspect.getfullargspec(py_func)
        min_arity = len(spec.args)
        if spec.defaults is not None:
            min_arity -= len(spec.defaults)

        max_arity: Optional[int] = len(spec.args)
        if spec.varargs is not None:
            max_arity = None

        if max_arity == 0:
            raise ValueError("Cannot create function with no arguments")

        if spec.kwonlyargs:
            raise ValueError("Cannot create function with keyword-only arguments")

        def definition(args, stack, exec):
            if len(args) < min_arity:
                fstr = "Function takes no fewer than {} arguments but {} were provided"
                raise JsonQLTypeError(
                    fstr.format(min_arity, len(args))
                )
            if max_arity is not None and len(args) > max_arity:
                fstr = "Function takes no more than {} arguments but {} were provided"
                raise JsonQLTypeError(
                   fstr.format(max_arity, len(args))
                )
            py_args = [exec(arg, stack).to_python() for arg in args]
            return RuntimeValue.of(py_func(*py_args))
        return RuntimeValue.wrap_function_def(definition)

    @staticmethod
    def eq(a, b):
        if a.type != b.type:
            return False
        if a.type == RuntimeValueType.Null:
            return True
        elif a.type == RuntimeValueType.Boolean:
            return a.value == b.value
        elif a.type == RuntimeValueType.Number:
            return a.value == b.value
        elif a.type == RuntimeValueType.String:
            return a.value == b.value
        elif a.type == RuntimeValueType.Array:
            if len(a.value) != len(b.value):
                return False
            for i in range(len(a.value)):
                if not RuntimeValue.eq(a.value[i], b.value[i]):
                    return False
            return True
        elif a.type == RuntimeValueType.Object:
            if len(a.value) != len(b.value):
                return False
            for key, value in a.value.items():
                if key not in b.value:
                    return False
                if not RuntimeValue.eq(value, b.value[key]):
                    return False
            return True
        elif a.type == RuntimeValueType.Regex:
            return (
                a.value.pattern == b.value.pattern
                and a.value.flags == b.value.flags
                and a.modifiers == b.modifiers  # due to py not having global flag
            )
        elif a.type == RuntimeValueType.Function:
            # referential equality
            return a.value == b.value
        else:
            raise ValueError("Equality not yet implemented: " + str(a.type))

    @staticmethod
    def compare(a, b) -> int:
        """
        Compare two values
        """
        if a.type != b.type:
            raise ValueError("Cannot compare  values of different types")
        elif not a.comparable():
            raise ValueError("Cannot compare  values of type " + str(a.type))
        elif a.type == RuntimeValueType.Boolean:
            return int(a.value) - int(b.value)
        elif a.type == RuntimeValueType.Number:
            return a.value - b.value
        elif a.type == RuntimeValueType.String:
            return (a.value > b.value) - (a.value < b.value)
        else:
            raise OpenAnIssueIfYouGetThisError(
                "Cannot compare MistQL values of type " + str(a.type))

    def comparable(self) -> bool:
        """
        Check if the value is comparable
        """
        return self.type in (
            RuntimeValueType.Boolean,
            RuntimeValueType.Number,
            RuntimeValueType.String,
        )

    def __lt__(self, __o: object):
        if not isinstance(__o, RuntimeValue):
            raise ValueError("Cannot compare MistQL value to non-MistQL value")
        return RuntimeValue.of(self.compare(self, __o) < 0)

    def __le__(self, __o: object):
        if not isinstance(__o, RuntimeValue):
            raise ValueError("Cannot compare MistQL value to non-MistQL value")
        return RuntimeValue.of(self.compare(self, __o) <= 0)

    def __gt__(self, __o: object):
        if not isinstance(__o, RuntimeValue):
            raise ValueError("Cannot compare MistQL value to non-MistQL value")
        return RuntimeValue.of(self.compare(self, __o) > 0)

    def __ge__(self, __o: object):
        if not isinstance(__o, RuntimeValue):
            raise ValueError("Cannot compare MistQL value to non-MistQL value")
        return RuntimeValue.of(self.compare(self, __o) >= 0)

    def __eq__(self, __o: object):
        if not isinstance(__o, RuntimeValue):
            raise ValueError("Cannot compare MistQL value to non-MistQL value")
        return RuntimeValue.of(RuntimeValue.eq(self, __o))

    def __ne__(self, __o: object):
        if not isinstance(__o, RuntimeValue):
            raise ValueError("Cannot compare MistQL value to non-MistQL value")
        return RuntimeValue.of(not RuntimeValue.eq(self, __o))

    def __bool__(self):
        return self.truthy()

    def __init__(self, type, value=None, modifiers=None):
        self.type = type
        self.value = value
        self.modifiers: Dict[str, Any] = modifiers if modifiers else {}

    def to_python(self):
        """
        Convert a MistQL RuntimeValue into a Python value
        """
        if self.type == RuntimeValueType.Null:
            return None
        elif self.type == RuntimeValueType.Boolean:
            return self.value
        elif self.type == RuntimeValueType.Number:
            return self.value
        elif self.type == RuntimeValueType.String:
            return self.value
        elif self.type == RuntimeValueType.Array:
            return [item.to_python() for item in self.value]
        elif self.type == RuntimeValueType.Object:
            return {key: value.to_python() for key, value in self.value.items()}
        else:
            raise ValueError(
                "Cannot convert MistQL value type to Python: " + str(self.type)
            )

    def truthy(self) -> bool:
        """
        Return whether this value is truthy
        """
        if self.type == RuntimeValueType.Null:
            return False
        elif self.type == RuntimeValueType.Boolean:
            return self.value
        elif self.type == RuntimeValueType.Number:
            return bool(self.value)
        elif self.type == RuntimeValueType.String:
            return self.value != ""
        elif self.type == RuntimeValueType.Array:
            return len(self.value) > 0
        elif self.type == RuntimeValueType.Object:
            return len(self.value) > 0
        elif self.type == RuntimeValueType.Function:
            return True
        elif self.type == RuntimeValueType.Regex:
            return True
        else:
            raise ValueError("Truthiness not yet implemented: " + str(self.type))

    def to_json(self, permissive=False) -> str:
        """
        Convert this value to JSON string
        """
        if self.type == RuntimeValueType.Null:
            return "null"
        elif self.type == RuntimeValueType.Boolean:
            return "true" if self.value else "false"
        elif self.type == RuntimeValueType.Number:
            num = self.value
            if num == int(num):
                num = int(num)
            return str(num)
        elif self.type == RuntimeValueType.String:
            return json.dumps(self.value)
        elif self.type == RuntimeValueType.Array:
            return (
                "[" + ",".join([item.to_json(permissive) for item in self.value]) + "]"
            )
        elif self.type == RuntimeValueType.Object:
            return (
                "{"
                + ",".join(
                    [
                        json.dumps(key) + ":" + item.to_json(permissive)
                        for key, item in self.value.items()
                    ]
                )
                + "}"
            )
        elif permissive:
            if self.type == RuntimeValueType.Function:
                return "[function]"
            elif self.type == RuntimeValueType.Regex:
                return "[regex]"
            else:
                return "[unknown]"
        raise ValueError("Cannot convert MistQL value to JSON: " + str(self.type))

    def to_string(self) -> str:
        """
        Convert this value to a string
        """
        if self.type == RuntimeValueType.String:
            return self.value
        elif self.type == RuntimeValueType.Number:
            return format_number(self.value)
        else:
            return self.to_json()

    def to_float(self) -> float:
        if self.type == RuntimeValueType.Number:
            return self.value
        elif self.type == RuntimeValueType.String:
            return float(self.value)
        elif self.type == RuntimeValueType.Boolean:
            return float(self.value)
        elif self.type == RuntimeValueType.Null:
            return float(0)
        else:
            raise JsonQLTypeError(
                "Cannot convert MistQL value to float: " + str(self.type)
            )

    def __repr__(self) -> str:
        # return "<mistql>"
        return f"<mistql {self.to_json(permissive=True)}>"

    def keys(self):
        if self.type == RuntimeValueType.Object:
            return [key for key in self.value]
        else:
            return []

    def access(self, string):
        """
        Access a property of this value
        """
        if self.type == RuntimeValueType.Object and string in self.value:
            return self.value[string]
        else:
            return RuntimeValue(RuntimeValueType.Null)


def assert_type(
    value: RuntimeValue, expected_type: Union[Set[RuntimeValueType], RuntimeValueType]
):
    if isinstance(expected_type, Set):
        if value.type not in expected_type:
            raise JsonQLTypeError(f"Expected one of {expected_type}, got {value.type}")
    else:
        if value.type != expected_type:
            raise JsonQLTypeError(f"Expected {expected_type}, got {value.type}")
    return value


def assert_int(value: RuntimeValue):
    value = assert_type(value, RuntimeValueType.Number)
    if value.value != int(value.value):
        raise JsonQLTypeError(f"Expected integer, got {value.value}")
    return value