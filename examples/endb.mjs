#!/usr/bin/env node

// MIT License

// Copyright (c) 2023 Håkan Råberg and Steven Deobald

// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice (including the next paragraph) shall be included in all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

/* global BigInt */

function fromJSONLD(x) {
    if (Object.hasOwn(x, '@type') && Object.hasOwn(x, '@value')) {
        const t = x['@type'];
        if (t === 'xsd:date' || t === 'xsd:dateTime') {
            return new Date(x['@value']);
        } else if (t === 'xsd:base64Binary') {
            return Uint8Array.from(atob(x['@value']), (m) => m.codePointAt(0));
        } else if (t === 'xsd:integer') {
            return BigInt(x['@value']);
        }
    }
    if (Object.hasOwn(x, '@graph')) {
        return x['@graph'];
    }
    return x;
}

function toJSONLD(x) {
    switch (x.constructor) {
        case Date:
            return {'@type': 'xsd:dateTime', '@value': x.toISOString()};
        case Uint8Array:
            var b = Array.from(x, (y) => String.fromCodePoint(y)).join('');
            return {'@type': 'xsd:base64Binary', '@value': btoa(b)};
        case BigInt:
            return {'@type': 'xsd:integer', '@value': x.toString()};
        case Array:
            return x.map(toJSONLD);
        case Object:
            return Object.fromEntries(Object.entries(x).map(([k, v]) => [k, toJSONLD(v)]));
        default:
            return x;
    }
}

class Endb {
    constructor(url = 'http://localhost:3803/sql', {accept = 'application/ld+json', username, password} = {}) {
        this.url = url;
        this.accept = accept;
        this.username = username;
        this.password = password;
    }

    async sql(q, p = [], m = false, accept) {
        const body = new FormData();

        body.append('q', q);
        body.append('p', JSON.stringify(toJSONLD(p)));
        body.append('m', JSON.stringify(m));

        accept = accept || this.accept;
        const headers = {'Accept': accept};

        if (this.username && this.password) {
            headers['Authorization'] = 'Basic ' + btoa(this.username + ":" + this.password);
        }

        const response = await fetch(this.url, {method: 'POST', headers: headers, body: body});

        if (response.ok) {
            if (accept === 'text/csv') {
                return await response.text();
            } else if (accept === 'application/vnd.apache.arrow.file') {
                return response.body;
            } else {
                return JSON.parse(await response.text(), (k, v) => fromJSONLD(v));
            }
        } else {
            throw new Error(response.status + ': ' + response.statusText + '\n' + await response.text());
        }
    }
}

class EndbWebSocket {
    constructor(url = 'ws://localhost:3803/sql', {ws, username, password} = {}) {
        this.ws = ws;
        if (typeof ws === 'undefined' && typeof WebSocket !== 'undefined') {
            this.ws = WebSocket;
        }
        this.conn = null;
        this.url = url;
        this.username = username;
        this.password = password;
        this.id = 1;
        this.sentMessages = {};
        this.pendingMessages = [];
    }

    async sql(q, p = [], m = false) {
        if (this.conn === null) {
            if (this.username && this.password) {
                this.conn = new this.ws(this.url, encodeURIComponent('Basic ' + btoa(this.username + ":" + this.password)));
            } else {
                this.conn = new this.ws(this.url);
            }
            this.conn.onerror = (event) => {
                console.log(event);
            };

            this.conn.onopen = () => {
                while (this.pendingMessages.length > 0) {
                    this.conn.send(this.pendingMessages.shift());
                }
            };

            this.conn.onclose = () => {
                for (const promise of Object.values(this.sentMessages)) {
                    promise.reject(new Error('Closed'));
                }
            };

            this.conn.onmessage = async (event) => {
                const message = JSON.parse(await event.data.text(), (k, v) => fromJSONLD(v));
                if (message['id'] === null) {
                    console.log(message);
                } else {
                    const id = message['id'].toString();
                    const promise = this.sentMessages[id];
                    if (promise !== null) {
                        delete this.sentMessages[id];
                        if (message.error) {
                            promise.reject(new Error(message['error']['message']));
                        } else {
                            promise.resolve(message['result']);
                        }
                    }
                }
            };
        }

        const message = {jsonrpc: '2.0', id: this.id++, method: 'sql', params: {q: q, p: p, m: m}};

        let resolve, reject;
        const promise = new Promise((res, rej) => {
            resolve = res;
            reject = rej;
        });

        const id = message.id.toString();
        this.sentMessages[id] = {promise: promise, resolve: resolve, reject: reject};

        const jsonMessage = JSON.stringify(toJSONLD(message));

        if (this.conn.readyState === 1) {
            this.conn.send(jsonMessage);
        } else if (this.conn.readyState === 0) {
            this.pendingMessages.push(jsonMessage);
        } else {
            reject(new Error('Closed'));
        }

        return promise;
    }
}

export { Endb, EndbWebSocket };

if (typeof process !== 'undefined' && import.meta.url === `file://${process.argv[1]}`) {
    console.log(await new Endb().sql(process.argv[2]));
}
