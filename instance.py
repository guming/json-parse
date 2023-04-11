from json_ql_parse import parse


class JsonQLInstance:
    def __init__(self) -> None:
        super().__init__()

    def query(self, query, data):
        ast = parse(query)
        data = input_garden_wall(data)
        result = execute_outer(ast, data, self.extras)
        return_value = output_garden_wall(result)
        return return_value
instance = JsonQLInstance();