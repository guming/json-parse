import json

import parse

with open('./data/testdata.json', 'r') as tsfile:
    data = json.load(tsfile)

print(data)
query = 'data | groupby describe | keys'
print('result:', parse.query(query, data))

tsfile.close()

