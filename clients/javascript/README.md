# @endatabas/endb

The official JavaScript client library for
[Endatabas](https://www.endatabas.com/).

## Requirements

* Node.js 14.5.0+ or other JavaScript engine with
  [strong ES2020 support](https://compat-table.github.io/compat-table/es2016plus/)

## Install

```sh
npm install @endatabas/endb
npm install ws
```

`@endatabas/endb` has zero dependencies. If you need WebSocket support
under Node.js, you will need to install a WebSocket library manually, as above.
If you are using `@endatabas/endb` in the browser (after bundling),
this is not required.

Please note that there is another (unscoped) packaged named `endb`.
It is not related to the Endatabas project.

## Documentation

* [JavaScript client library](https://docs.endatabas.com/reference/clients#javascript)
* [Endb SQL Reference](https://docs.endatabas.com/sql/)

## Usage

```javascript
import WebSocket from 'ws';
import { Endb, EndbWebSocket } from '@endatabas/endb';

var e = new Endb();
await e.sql("insert into users {name: 'Thupil'};");
var result = await e.sql("select * from users;");
console.log(result);

var ews = new EndbWebSocket({ws: WebSocket});
await ews.sql("insert into users {name: 'Lydia'};");
var ws_result = await ews.sql("select * from users;");
console.log(ws_result);
```

## Usage (Web)

If you want to try out Endb from the browser without Node.js
or bundling, you can consume it directly.

Note that this approach is only really useful for quick
experimentation in the browser's JavaScript console, not
production usage.
Without a proxy, it's most likely that only `EndbWebSocket`
will work without CORS errors, as the `Endb` class relies on HTTP.

```javascript
// endb_sample.mjs
import { Endb, EndbWebSocket } from 'path/to/node_modules/@endatabas/endb/endb.mjs';
window.Endb = Endb;
window.EndbWebSocket = EndbWebSocket;
```

```html
<!-- endb_sample.html -->
<html>
  <head><script type="module" src="endb_sample.mjs"></script></head>
  <body><h1>Endb Sample</h1></body>
</html>
```

```javascript
// in the endb_sample.html JavaScript console:
var ews = new window.EndbWebSocket();
ews.sql(`select * from ${table}`, {table: 'users'});
```

## Generating Documentation

This is only required if you are updating
[docs.endatabas.com](https://docs.endatabas.com):

```sh
# install jsdoc2md
npm install -g jsdoc
npm install -g jsdoc-to-markdown

# clone 'endb-book' (docs.endatabas.com)
cd .. && git clone git@github.com:endatabas/endb-book.git && cd endb
# create the docs (copies to 'endb-book')
make doc
```

## Copyright and License

This subdirectory of the main Git repository and the contents of
the `@endatabas/endb` NPM package are subject to the MIT License:

Copyright 2023-2024 Håkan Råberg and Steven Deobald.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
