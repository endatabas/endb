#!/usr/bin/env python3

import base64
from datetime import date, datetime, time
import json
import urllib.parse
import urllib.request

class Endb:
    def __init__(self, url='http://localhost:3803/sql', accept='application/ld+json', username=None, password=None):
        super().__init__()
        self.url = url
        self.accept = accept
        self.username = username
        self.password = password

    def _from_json_ld(self, obj):
        match obj.get('@type', None):
            case 'xsd:dateTime':
                return datetime.fromisoformat(obj['@value'].replace('Z', '+00:00'))
            case 'xsd:date':
                 return date.fromisoformat(obj['@value'])
            case 'xsd:time':
                 return time.fromisoformat(obj['@value'])
            case 'xsd:base64Binary':
                 return base64.b64decode(obj['@value'])
            case 'xsd:integer':
                 return int(obj['@value'])
            case _:
                 return obj.get('@graph', obj)

    def _to_json_ld(self, obj):
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
                 raise TypeError

    def sql(self, q, p=[], m=False, accept=None):
        if accept is None:
            accept = self.accept
        headers = {'Accept': accept}
        if self.username is not None and self.password is not None:
            auth_base64 = base64.b64encode(bytes('%s:%s' % (self.username, self.password), 'ascii'))
            headers['Authorization'] = 'Basic %s' % auth_base64.decode('utf-8')

        payload = {'q': q, 'p': json.dumps(p, default=self._to_json_ld), 'm': json.dumps(m)}
        data = urllib.parse.urlencode(payload)
        data = data.encode('ascii')

        req = urllib.request.Request(self.url, data, headers, method='POST')
        with urllib.request.urlopen(req) as response:
            if accept == 'text/csv':
                return response.read().decode()
            elif accept == 'application/vnd.apache.arrow.file':
                return response.read()
            else:
                return json.loads(response.read(), object_hook=self._from_json_ld)

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        print(Endb().sql(sys.argv[1]))
