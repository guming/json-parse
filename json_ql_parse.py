from lark import Lark

json_ql_parser = Lark.open("grammar.lark", rel_to=__file__, parser="earley")
def parse(raw):
    # TODO: Translate errors from this function to something that inherits
    # from MistQLException
    parsed = json_ql_parser.parse(raw)
    return from_lark(parsed)