#!/usr/bin/env python3

import base64
from datetime import date, datetime, time
import json
import urllib.parse
import urllib.request

def from_json_ld(obj):
    match obj.get('@type', None):
        case 'xsd:dateTime':
            return datetime.fromisoformat(obj['@value'].replace('Z', '+00:00'))
        case 'xsd:date':
            return date.fromisoformat(obj['@value'])
        case 'xsd:time':
            return time.fromisoformat(obj['@value'])
        case 'xsd:base64Binary':
            return base64.b64decode(obj['@value'])
        case 'xsd:long':
            return int(obj['@value'])
        case _:
            return obj.get('@graph', obj)

class JSONLDEncoder(json.JSONEncoder):
    def default(self, obj):
        match obj:
           case datetime():
               return {'@value': datetime.isoformat(obj), '@type': 'xsd:dateTime'}
           case date():
               return {'@value': date.isoformat(obj), '@type': 'xsd:date'}
           case time():
               return {'@value': time.isoformat(obj), '@type': 'xsd:time'}
           case bytes():
               return {'@value': base64.b64encode(obj).decode(), '@type': 'xsd:base64Binary'}
           case _:
               return super().default(obj)

def sql(q, p=[], accept='application/ld+json', auth=None, url='http://localhost:3803/sql'):
    headers = {'Accept': accept}
    if auth and len(auth) == 2:
        auth_base64 = base64.b64encode(bytes('%s:%s' % auth, 'ascii'))
        headers['Authorization'] = 'Basic %s' % auth_base64.decode('utf-8')

    payload = {'q': q, 'p': json.dumps(p, cls=JSONLDEncoder)}
    data = urllib.parse.urlencode(payload)
    data = data.encode('ascii')

    req = urllib.request.Request(url, data, headers, method='POST')
    with urllib.request.urlopen(req) as response:
        if accept == 'text/csv':
            return response.read().decode()
        else:
            return json.loads(response.read(), object_hook=from_json_ld)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        print(sql(sys.argv[1]))
