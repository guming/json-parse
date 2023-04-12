from lark import Lark, Transformer, v_args

class TreeToJson(Transformer):
    @v_args(inline=True)
    def string(self, s):
        return s[1:-1].replace('\\"', '"')

    array = list
    pair = tuple
    object = dict
    number = v_args(inline=True)(float)

    null = lambda self, _: None
    true = lambda self, _: True
    false = lambda self, _: False


json_parser = Lark.open("json_grammer.lark", parser='lalr', rel_to=__file__,
                        lexer='basic',
                        propagate_positions=False,
                        maybe_placeholders=False,
                        transformer=TreeToJson()
                        )

parse = json_parser.parse

def test():
    test_json = '''
        {
            "empty_object" : {},
            "empty_array"  : [],
            "booleans"     : { "YES" : true, "NO" : false },
            "numbers"      : [ 0, 1, -2, 3.3, 4.4e5, 6.6e-7 ],
            "strings"      : [ "This", [ "And" , "That", "And a \\"b" ] ],
            "nothing"      : null
        }
    '''

    j = parse(test_json)
    # print(j.pretty())
    import json
    assert j == json.loads(test_json)


if __name__ == '__main__':
    test()
    # with open(sys.argv[1]) as f:
    #     print(parse(f.read()))