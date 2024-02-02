# Endatabas JavaScript Client

## Requirements

* Node.js 14+ (if using node)
* A JavaScript version which supports the Nullish Coalescing Operator (`??`)

## Usage

Once you have an `endb` module available, you can instantitate
`Endb` and `EndbWebSocket` classes, which have `sql` methods
on them.
See below for various options to import the `endb` module.

```javascript
var e = new endb.Endb();
# var e = new endb.EndbWebSocket();
e.sql("insert into users {name: 'Thupil'};");
e.sql("select * from users;").then(result => { console.log(result) });
```

## Import - NPM package

From a file:

```javascript
import endb from '@endatabas/endb';
```

From a node.js REPL:

```
let endb;
import("@endatabas/endb").then(module => { endb = module });
```

## Import - local file

Inside a JavaScript file:

```javascript
import endb from './endb.mjs';
```

From a node.js REPL:

```javascript
let endb;
import("./endb.mjs").then(module => { endb = module });
```
