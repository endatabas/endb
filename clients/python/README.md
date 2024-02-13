# Endatabas Console and Python Client

The official console and Python client library for
[Endatabas](https://www.endatabas.com).

## Installation

```sh
pip install endb
pip install websockets  # optional WebSocket support
pip install pyarrow     # optional Apache Arrow support
```

`endb` is supported on Python 3.10 and above.
[WebSocket](https://docs.endatabas.com/reference/websocket_api)
support is optional.

## Documentation

* [Endb Console](https://docs.endatabas.com/reference/console)
* [Python client library](https://docs.endatabas.com/reference/clients#python)
* [Endb SQL Reference](https://docs.endatabas.com/sql/)

## Console Usage

```sh
endb_console [--url URL] [-u USERNAME] [-p PASSWORD]
```

## Python Client Library Usage

```python
from endb import Endb
e = Endb()
e.sql("INSERT INTO users {name: 'Yuvi'}")
e.sql("SELECT * FROM users;")
```

When the `websockets` dependency is installed, it is possible to
return asynchronous results to the Python interactive shell
directly if you start it with `python3 -m asyncio`:

```python
from endb import EndbWebSocket
ews = EndbWebSocket()
result = await ews.sql("SELECT * FROM users;")
print(result)
```

## Development

```sh
pip install setuptools wheel twine
make
```

## Copyright and License

This subdirectory of the main Git repository and the contents of
the `endb` PyPI package are subject to the MIT License:

Copyright 2023-2024 Håkan Råberg and Steven Deobald.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
