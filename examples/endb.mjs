#!/usr/bin/env node

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
            const b = Array.from(x, (y) => String.fromCodePoint(y)).join('');
            return {'@type': 'xsd:base64Binary', '@value': btoa(b)};
        case BigInt:
            return {'@type': 'xsd:integer', '@value': x.toString()};
        case Array:
            return x.map(toJSONLD);
        case Object:
            return Object.fromEntries(Object.entries(x).map(([k, v], i) => [k, toJSONLD(v)]));
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

    async sql(q, p = [], accept) {
        const body = new FormData();

        body.append('q', q);
        body.append('p', JSON.stringify(toJSONLD(p)))

        accept = accept || this.accept;
        const headers = {'Accept': accept};

        if (this.username && this.password) {
            headers['Authorization'] = 'Basic ' + btoa(this.username + ":" + this.password);
        }

        const response = await fetch(this.url, {method: 'POST', headers: headers, body: body});

        if (response.ok) {
            if (accept === 'text/csv') {
                return await response.text();
            } else {
                return JSON.parse(await response.text(), (k, v) => fromJSONLD(v));
            }
        } else {
            throw new Error(response.status + ': ' + response.statusText + '\n' + await response.text());
        }
    }
}

export { Endb };

if (typeof process !== 'undefined' && import.meta.url === `file://${process.argv[1]}`) {
    console.log(await new Endb().sql(process.argv[2]));
}
