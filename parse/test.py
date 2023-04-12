import json

import parse

with open('/Users/guming/PycharmProjects/json-query/data/testdata.json', 'r') as tsfile:
    data = json.load(tsfile)

print(data)
query = 'events | groupby type | keys'
print('result:',parse.query(query, data))

tsfile.close()

