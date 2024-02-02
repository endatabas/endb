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

## Resources

* [Documentation](https://docs.endatabas.com/reference/clients#javascript)

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
This approach is only really useful for quick experimentation
in the browser's JavaScript console.

```javascript
# endb_sample.mjs
import { Endb, EndbWebSocket } from 'path/to/node_modules/@endatabas/endb/endb.mjs';
window.Endb = Endb;
window.EndbWebSocket = EndbWebSocket;
```

```html
<html>
  <head><script type="module" src="endb_sample.mjs"></script></head>
  <body><h1>Endb Sample</h1></body>
</html>
```
