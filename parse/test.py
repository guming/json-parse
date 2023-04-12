import json

import parse

with open('./data/testdata.json', 'r') as tsfile:
    data = json.load(tsfile)

print(data)
query = 'events | filter type == "send_message" '
print('result:', parse.query(query, data))

tsfile.close()

