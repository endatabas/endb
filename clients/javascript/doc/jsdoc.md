#### Classes

<dl>
<dt><a href="#Endb">Endb</a></dt>
<dd><p>Endatabas client for the HTTP API</p>
</dd>
<dt><a href="#EndbWebSocket">EndbWebSocket</a></dt>
<dd><p>Endatabas client for the WebSocket API</p>
</dd>
</dl>

<a name="Endb"></a>

#### Endb
Endatabas client for the HTTP API

**Kind**: global class  

* [Endb](#Endb)
    * [new Endb([url], [opt])](#new_Endb_new)
    * [.sql(q, [p], [m], [accept])](#Endb+sql) ⇒ <code>Promise.&lt;Array&gt;</code>

<a name="new_Endb_new"></a>

##### new Endb([url], [opt])
Create an Endb object (Endatabas client for the HTTP API)


| Param | Type | Default | Description |
| --- | --- | --- | --- |
| [url] | <code>string</code> | <code>&quot;http://localhost:3803/sql&quot;</code> | HTTP URL to the Endatabas /sql API |
| [opt] | <code>Object</code> |  | HTTP options |
| [opt.accept] | <code>string</code> | <code>&quot;application/ld+json&quot;</code> | Accept Header content type |
| [opt.username] | <code>string</code> |  | username for HTTP Basic Auth |
| [opt.password] | <code>string</code> |  | password for HTTP Basic Auth |

<a name="Endb+sql"></a>

##### endb.sql(q, [p], [m], [accept]) ⇒ <code>Promise.&lt;Array&gt;</code>
Execute a SQL statement over HTTP

**Kind**: instance method of [<code>Endb</code>](#Endb)  
**Returns**: <code>Promise.&lt;Array&gt;</code> - - Array of documents  

| Param | Type | Description |
| --- | --- | --- |
| q | <code>string</code> | SQL query as string or Template Literal |
| [p] | <code>Array</code> | Positional parameters, named parameters, or an array of either |
| [m] | <code>boolean</code> | many parameters flag |
| [accept] | <code>string</code> | Accept Header content type |

**Example**  
```js
// Simple query
sql("SELECT * FROM users;");
// Positional parameters
sql("INSERT INTO users (date, name) VALUES (?, ?);", [new Date(), 'Aaron']);
// Named parameters
sql("INSERT INTO users {date: :d, name: :n};", {d: new Date(), n: 'Aaron'});
// Many positional parameters (batches)
sql("INSERT INTO users (name) VALUES (?);", [['Aaron'], ['Kurt'], ['Cindy']], true);
// Many named parameters (batches)
sql("INSERT INTO users {name: :n};", [{n: 'Judy'}, {n: 'Addis'}], true);
// All parameters (many parameters and accept header)
sql("INSERT INTO users (name) VALUES (?);", [['Aaron'], ['Kurt'], ['Cindy']], true, 'text/csv');
// Named parameters via Template Literals
sql(`INSERT INTO users (name) VALUES (${u});`, [{u: 'Michael'}]);
```
<a name="EndbWebSocket"></a>

#### EndbWebSocket
Endatabas client for the WebSocket API

**Kind**: global class  

* [EndbWebSocket](#EndbWebSocket)
    * [new EndbWebSocket([url], [opt])](#new_EndbWebSocket_new)
    * [.sql(q, [p], [m], [accept])](#EndbWebSocket+sql) ⇒ <code>Promise.&lt;Array&gt;</code>

<a name="new_EndbWebSocket_new"></a>

##### new EndbWebSocket([url], [opt])
Create an EndbWebSocket object (Endatabas client for the WebSocket API)


| Param | Type | Default | Description |
| --- | --- | --- | --- |
| [url] | <code>string</code> | <code>&quot;ws://localhost:3803/sql&quot;</code> | WebSocket URL to the Endatabas /sql API |
| [opt] | <code>Object</code> |  | WebSocket options |
| [opt.ws] | <code>string</code> |  | WebSocket implementation |
| [opt.username] | <code>string</code> |  | username for Basic Auth |
| [opt.password] | <code>string</code> |  | password for Basic Auth |

<a name="EndbWebSocket+sql"></a>

##### endbWebSocket.sql(q, [p], [m], [accept]) ⇒ <code>Promise.&lt;Array&gt;</code>
Execute a SQL statement over a WebSocket with LD-JSON

**Kind**: instance method of [<code>EndbWebSocket</code>](#EndbWebSocket)  
**Returns**: <code>Promise.&lt;Array&gt;</code> - - Array of documents  

| Param | Type | Description |
| --- | --- | --- |
| q | <code>string</code> | SQL query as string or Template Literal |
| [p] | <code>Array</code> | Positional parameters, named parameters, or an array of either |
| [m] | <code>boolean</code> | many parameters flag |
| [accept] | <code>string</code> | Accept Header content type |

**Example**  
```js
// Simple query
sql("SELECT * FROM users;");
// Positional parameters
sql("INSERT INTO users (date, name) VALUES (?, ?);", [new Date(), 'Aaron']);
// Named parameters
sql("INSERT INTO users {date: :d, name: :n};", {d: new Date(), n: 'Aaron'});
// Many positional parameters (batches)
sql("INSERT INTO users (name) VALUES (?);", [['Aaron'], ['Kurt'], ['Cindy']], true);
// Many named parameters (batches)
sql("INSERT INTO users {name: :n};", [{n: 'Judy'}, {n: 'Addis'}], true);
// All parameters (many parameters and accept header)
sql("INSERT INTO users (name) VALUES (?);", [['Aaron'], ['Kurt'], ['Cindy']], true, 'text/csv');
// Named parameters via Template Literals
sql(`INSERT INTO users (name) VALUES (${u});`, [{u: 'Michael'}]);
```
