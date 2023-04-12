import json
from typing import Union, Any, List

from lark import Lark, Tree, Token

from exception import OpenAnIssueIfYouGetThisError
from expression import BaseExpression, ValueExpression, RefExpression, ArrayExpression, ObjectExpression, \
    PipeExpression, FnExpression

json_ql_parser = Lark.open("json_query_grammar.lark", rel_to=__file__, parser="earley")
function_mappings = {
    "dot": ".",
    "neg": "-/unary",
    "not": "!/unary",
    "eq": "==",
    "neq": "!=",
    "match": "=~",
    "and": "&&",
    "or": "||",
}
def parse(raw):
    parsed = json_ql_parser.parse(raw)
    return from_lark(parsed)

def from_lark(lark_node: Union[Any,str,Tree,Token]):
    print(lark_node)
    if isinstance(lark_node,Token):
        return process_lark_token(lark_node)
    if isinstance(lark_node,Tree):
        return process_lark_tree(lark_node)
    else:
        raise OpenAnIssueIfYouGetThisError(f"Unknown lark node type: {type(lark_node)}")

def process_lark_token(lark_node: Token) -> BaseExpression:
    if lark_node.type == "NUMBER":
        return ValueExpression.of(float(lark_node.value))
    elif lark_node.type == "ESCAPED_STRING":
        value = json.loads(lark_node.value)
        return ValueExpression.of(value)
    elif lark_node.type == "TRUE":
        return ValueExpression.of(True)
    elif lark_node.type == "FALSE":
        return ValueExpression.of(False)
    elif lark_node.type == "NULL":
        return ValueExpression.of(None)
    elif lark_node.type == "CNAME":
        return RefExpression(lark_node.value)
    elif lark_node.type == "AT":
        return RefExpression("@")
    elif lark_node.type == "DOLLAR":
        return RefExpression("$")
    else:
        raise OpenAnIssueIfYouGetThisError(f"Unknown token type {lark_node.type}")

def process_lark_tree(lark_node) -> BaseExpression:
    # print(lark_node)
    print(lark_node.data)
    if lark_node.data == "array":
        return ArrayExpression([from_lark(item) for item in lark_node.children])
    elif lark_node.data == "object":
        obj = {}
        for child in lark_node:
            if isinstance(child,str):
                raise OpenAnIssueIfYouGetThisError(
                    "Got string for child when we didn't expect it."
                )
            key = from_lark(child.children[0])
            if isinstance(key, ValueExpression):
                key = key.value.value
            elif isinstance(key, RefExpression):
                key = key.name
            else:
                raise OpenAnIssueIfYouGetThisError(f"Unknown key type {type(key)}")
            value = from_lark(child.children[1])
            obj[key] = value
        return ObjectExpression(obj)
    elif lark_node.data == "pipe":
        return PipeExpression([from_lark(child) for child in lark_node.children])
    elif lark_node.data == "fncall":
        return FnExpression(
            from_lark(lark_node.children[0]),
            [from_lark(child) for child in lark_node.children[1:]])
    elif lark_node.data in function_mappings:
        return FnExpression(
            RefExpression(function_mappings[lark_node.data], absolute=True),
            [from_lark(child) for child in lark_node.children[:]]
        )
    elif lark_node.data == "index":
        # This is gross becase i can't figure out how to get the tree to look
        # a little more sensible.
        base, indexing = lark_node.children
        if isinstance(indexing, str):
            raise OpenAnIssueIfYouGetThisError(
                "Got string for child when we didn't expect it."
            )
        innards = indexing.children[0]
        if isinstance(innards, str):
            raise OpenAnIssueIfYouGetThisError(
                "Got string for child when we didn't expect it."
            )
        fnexp_args: List[BaseExpression] = []
        prev_was_token = True
        for child in innards.children:
            if isinstance(child, Token) and child.value == ":":
                if prev_was_token:
                    fnexp_args.append(ValueExpression.of(None))
                prev_was_token = True
                continue
            else:
                fnexp_args.append(from_lark(child))
                prev_was_token = False
        if prev_was_token:
            fnexp_args.append(ValueExpression.of(None))
        fnexp_args.append(from_lark(base))
        return FnExpression(RefExpression("index", absolute=True), fnexp_args)
    else:
        raise OpenAnIssueIfYouGetThisError(
            f"Unknown lark expression type: {lark_node.data}"
        )