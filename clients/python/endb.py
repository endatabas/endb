# MIT License

# Copyright (c) 2023-2024 Håkan Råberg and Steven Deobald

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice (including the next paragraph) shall be included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import base64
from datetime import date, datetime, time
import json
import urllib.parse
import urllib.request
import email
import email.policy

class AbstractEndb:
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

# https://pypi.org/project/pyarrow/
try:
    import pyarrow
except ModuleNotFoundError:
    pyarrow = None

class Endb(AbstractEndb):
    """
    An Endatabas client for the HTTP API

    Attributes
    ----------
    url : str
        HTTP URL of an Endatabas instance
    accept : str
        Accept header content type
    username : str
        Username for HTTP Basic Auth
    password : str
        Password for HTTP Basic Auth
    """
    def __init__(self, url='http://localhost:3803/sql', accept='application/ld+json', username=None, password=None):
        """
        Parameters
        ----------
        url : str, default='http://localhost:3803/sql'
            HTTP URL of an Endatabas instance
        accept : str, default='application/ld+json'
            Accept header content type
        username : str, default=None
            Username for HTTP Basic Auth
        password : str, default=None
            Password for HTTP Basic Auth
        """
        super().__init__()
        self.url = url
        self.accept = accept
        self.username = username
        self.password = password

    def sql(self, q, p=[], m=False, accept=None):
        """Executes a SQL statement

        The SQL statement is sent to `Endb.url` over HTTP.

        Parameters
        ----------
        q : str
            SQL statement or query to execute
        p : array_like, default=[]
            Positional or named SQL parameters (or an array of either, if using many parameters)
        m : bool, default=False
            Many parameters flag
        accept : str, optional
            Accept header content type (defaults to `Endb.accept`)

        Raises
        ------
        TypeError
            Internal error if attempting to translate an unknown type
            to LD-JSON.
        """
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
                if pyarrow is None:
                    return response.read()
                else:
                     with pyarrow.ipc.open_file(response.read()) as reader:
                         return [reader.get_batch(n) for n in range(reader.num_record_batches)]
            elif accept == 'application/vnd.apache.arrow.stream':
                if pyarrow is None:
                    return response.read()
                else:
                     with pyarrow.ipc.open_stream(response.read()) as reader:
                         return [b for b in reader]
            elif accept == 'multipart/mixed':
                body = 'Content-Type: ' + response.headers['Content-Type'] + '\r\n\r\n' + response.read().decode()
                msg = email.message_from_string(body, policy=email.policy.HTTP)
                return [json.loads(part.get_content(), object_hook=self._from_json_ld) for part in msg.iter_parts()]
            else:
                return json.loads(response.read(), object_hook=self._from_json_ld)

# https://pypi.org/project/websockets/
try:
    import websockets

    class EndbWebSocket(AbstractEndb):
        """
        An Endatabas client for the HTTP API

        Attributes
        ----------
        url : str
            HTTP URL of an Endatabas instance
        username : str
            Username for HTTP Basic Auth
        password : str
            Password for HTTP Basic Auth
        """
        def __init__(self, url='ws://localhost:3803/sql', username=None, password=None):
            """
            Parameters
            ----------
            url : str, default='ws://localhost:3803/sql'
                HTTP URL of an Endatabas instance
            username : str, default=None
                Username for HTTP Basic Auth
            password : str, default=None
                Password for HTTP Basic Auth
            """
            super().__init__()
            self.url = url
            self.username = username
            self.password = password
            self.id = 1
            self.ws = None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            await self.close()

        async def close(self):
            """Closes the WebSocket connection"""
            if self.ws is not None:
                await self.ws.close()

        async def sql(self, q, p=[], m=False, accept=None):
            """Executes a SQL statement

            The SQL statement is sent to `Endb.url` over WebSockets.

            Parameters
            ----------
            q : str
                SQL statement or query to execute
            p : array_like, default=[]
                Positional or named SQL parameters (or an array of either, if using many parameters)
            m : bool, default=False
                Many parameters flag
            accept : str, optional
                Ignored. WebSocket communication is always in LD-JSON.

            Raises
            ------
            AssertionError
                If 'id' of request and response do not match.
            RuntimeError
                If response from server contains an error.
            """
            if self.ws is None:
                if self.username is not None and self.password is not None:
                    auth_base64 = base64.b64encode(bytes('%s:%s' % (self.username, self.password), 'ascii'))
                    subprotocol = urllib.parse.quote('Basic %s' % auth_base64.decode('utf-8'))
                    self.ws = await websockets.connect(self.url, subprotocols=[subprotocol])
                else:
                    self.ws = await websockets.connect(self.url)

            request = {'jsonrpc': '2.0', 'id': self.id, 'method': 'sql', 'params': {'q': q, 'p': p, 'm': m}}
            self.id = self.id + 1

            await self.ws.send(json.dumps(request, default=self._to_json_ld))

            response = json.loads(await self.ws.recv(), object_hook=self._from_json_ld)

            if 'id' in request and 'id' in response:
                assert request['id'] == response['id']

            if 'error' in response:
                raise RuntimeError(response['error']['message'])
            else:
                return response['result']

except ModuleNotFoundError:
    pass
