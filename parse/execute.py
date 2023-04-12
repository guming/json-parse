from typing import List

from typeguard import typechecked

from parse.builtins import builtins, FunctionDefinitionType
from exception import OpenAnIssueIfYouGetThisError, JsonQLTypeError
from expression import BaseExpression
from expression import (
    RefExpression,
    FnExpression,
    ValueExpression,
    ArrayExpression,
    ObjectExpression,
    PipeExpression,
)
from runtime_value import RuntimeValue, RuntimeValueType
from stack import StackFrame, find_in_stack, build_initial_stack, Stack, add_runtime_value_to_stack

def execute_outer(
    ast: BaseExpression,
    data: RuntimeValue,
    extras: StackFrame
) -> RuntimeValue:
    return execute(ast, build_initial_stack(data, builtins, extras))
@typechecked
def execute(ast: BaseExpression,stack: Stack) -> RuntimeValue:
    if not isinstance(ast, BaseExpression):
        raise OpenAnIssueIfYouGetThisError(
            f"Expected to evaluate an expression, got {ast}"
        )
    if isinstance(ast, ValueExpression):
        return ast.value
    elif isinstance(ast, RefExpression):
        return find_in_stack(stack, ast.name, ast.absolute)
    elif isinstance(ast, FnExpression):
        return execute_fncall(ast.fn, ast.args, stack)
    elif isinstance(ast, ArrayExpression):
        return RuntimeValue.of([execute(item, stack) for item in ast.items])
    elif isinstance(ast, ObjectExpression):
        return RuntimeValue.of(
            {key: execute(value, stack) for key, value in ast.entries.items()}
        )
    elif isinstance(ast, PipeExpression):
        return execute_pipe(ast.stages, stack)
    raise NotImplementedError("execute() not implemented for " + str(ast.type))

@typechecked
def execute_fncall(head: BaseExpression, arguments: List[BaseExpression], stack: Stack):
    fn = execute(head, stack)
    if fn.type != RuntimeValueType.Function:
        raise JsonQLTypeError(f"Tried to call a non-function: {fn}")
    function_definition: FunctionDefinitionType = fn.value
    return function_definition(arguments, stack, execute)

@typechecked
def execute_pipe(stages: List[BaseExpression], stack: Stack) -> RuntimeValue:
    first: BaseExpression = stages[0]
    remaining: List[BaseExpression] = stages[1:]
    data = execute(first, stack)

    for stage_ast in remaining:
        new_stack = add_runtime_value_to_stack(data, stack)
        if not isinstance(stage_ast, FnExpression):
            raise OpenAnIssueIfYouGetThisError("Pipe stage is not a function!!")
        args: List[BaseExpression] = stage_ast.args.copy()
        args.append(ValueExpression(data))
        stage = FnExpression(stage_ast.fn, args)
        data = execute(stage, new_stack)

    return data