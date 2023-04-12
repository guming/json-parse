from lark import Lark

json_ql_parser = Lark.open("grammar.lark", rel_to=__file__, parser="lclr")

def parse(raw):
    parsed = json_ql_parser.parse(raw)
    return from_lark(parsed)