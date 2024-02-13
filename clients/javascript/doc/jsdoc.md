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
    * [`new Endb([url], [opt])`](#new_Endb_new)
    * [`.sql(q, [p], [m], [accept])`](#Endb+sql) ⇒ <code>Promise.&lt;Array&gt;</code>

<a name="new_Endb_new"></a>

##### `new Endb([url], [opt])`
Create an Endb object (Endatabas client for the HTTP API)

<table>
  <thead>
    <tr>
      <th>Param</th><th>Type</th><th>Default</th><th>Description</th>
    </tr>
  </thead>
  <tbody>
<tr>
    <td>[url]</td><td><code>string</code></td><td><code>&quot;http://localhost:3803/sql&quot;</code></td><td><p>HTTP URL to the Endatabas /sql API</p>
</td>
    </tr><tr>
    <td>[opt]</td><td><code>Object</code></td><td></td><td><p>HTTP options</p>
</td>
    </tr><tr>
    <td>[opt.accept]</td><td><code>string</code></td><td><code>&quot;application/ld+json&quot;</code></td><td><p>Accept Header content type</p>
</td>
    </tr><tr>
    <td>[opt.username]</td><td><code>string</code></td><td></td><td><p>username for HTTP Basic Auth</p>
</td>
    </tr><tr>
    <td>[opt.password]</td><td><code>string</code></td><td></td><td><p>password for HTTP Basic Auth</p>
</td>
    </tr>  </tbody>
</table>

<a name="Endb+sql"></a>

##### `endb.sql(q, [p], [m], [accept])` ⇒ <code>Promise.&lt;Array&gt;</code>
Execute a SQL statement over HTTP

**Kind**: instance method of [<code>Endb</code>](#Endb)  
**Returns**: <code>Promise.&lt;Array&gt;</code> - - Array of documents  
<table>
  <thead>
    <tr>
      <th>Param</th><th>Type</th><th>Description</th>
    </tr>
  </thead>
  <tbody>
<tr>
    <td>q</td><td><code>string</code></td><td><p>SQL query as string or Template Literal</p>
</td>
    </tr><tr>
    <td>[p]</td><td><code>Array</code></td><td><p>Positional parameters, named parameters, or an array of either</p>
</td>
    </tr><tr>
    <td>[m]</td><td><code>boolean</code></td><td><p>many parameters flag</p>
</td>
    </tr><tr>
    <td>[accept]</td><td><code>string</code></td><td><p>Accept Header content type</p>
</td>
    </tr>  </tbody>
</table>

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
    * [`new EndbWebSocket([url], [opt])`](#new_EndbWebSocket_new)
    * [`.sql(q, [p], [m], [accept])`](#EndbWebSocket+sql) ⇒ <code>Promise.&lt;Array&gt;</code>

<a name="new_EndbWebSocket_new"></a>

##### `new EndbWebSocket([url], [opt])`
Create an EndbWebSocket object (Endatabas client for the WebSocket API)

<table>
  <thead>
    <tr>
      <th>Param</th><th>Type</th><th>Default</th><th>Description</th>
    </tr>
  </thead>
  <tbody>
<tr>
    <td>[url]</td><td><code>string</code></td><td><code>&quot;ws://localhost:3803/sql&quot;</code></td><td><p>WebSocket URL to the Endatabas /sql API</p>
</td>
    </tr><tr>
    <td>[opt]</td><td><code>Object</code></td><td></td><td><p>WebSocket options</p>
</td>
    </tr><tr>
    <td>[opt.ws]</td><td><code>string</code></td><td></td><td><p>WebSocket implementation</p>
</td>
    </tr><tr>
    <td>[opt.username]</td><td><code>string</code></td><td></td><td><p>username for Basic Auth</p>
</td>
    </tr><tr>
    <td>[opt.password]</td><td><code>string</code></td><td></td><td><p>password for Basic Auth</p>
</td>
    </tr>  </tbody>
</table>

<a name="EndbWebSocket+sql"></a>

##### `endbWebSocket.sql(q, [p], [m], [accept])` ⇒ <code>Promise.&lt;Array&gt;</code>
Execute a SQL statement over a WebSocket with LD-JSON

**Kind**: instance method of [<code>EndbWebSocket</code>](#EndbWebSocket)  
**Returns**: <code>Promise.&lt;Array&gt;</code> - - Array of documents  
<table>
  <thead>
    <tr>
      <th>Param</th><th>Type</th><th>Description</th>
    </tr>
  </thead>
  <tbody>
<tr>
    <td>q</td><td><code>string</code></td><td><p>SQL query as string or Template Literal</p>
</td>
    </tr><tr>
    <td>[p]</td><td><code>Array</code></td><td><p>Positional parameters, named parameters, or an array of either</p>
</td>
    </tr><tr>
    <td>[m]</td><td><code>boolean</code></td><td><p>many parameters flag</p>
</td>
    </tr><tr>
    <td>[accept]</td><td><code>string</code></td><td><p>Accept Header content type</p>
</td>
    </tr>  </tbody>
</table>

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
