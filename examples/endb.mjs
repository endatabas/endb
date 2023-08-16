#!/usr/bin/env node

function jsonLDEncoder(x) {
    if (x instanceof Date) {
        return {'@type': 'xsd:dateTime', '@value': x.toISOString()};
    } else if (x instanceof Uint8Array) {
        const b = Array.from(x, (y) => String.fromCodePoint(y)).join('');
        return {'@type': 'xsd:base64Binary', '@value': btoa(b)};
    } else if (x instanceof Array) {
        return x.map(jsonLDEncoder);
    } else if (x instanceof Object) {
        return Object.fromEntries(Object.entries(x).map(([k, v], i) => [k, jsonLDEncoder(v)]));
    } else {
        return x;
    }
}

function jsonLDDecoder(x) {
    if (x instanceof Object && x['@type'] && x['@value']) {
        const t = x['@type'];
        if (t === 'xsd:date' || t === 'xsd:dateTime') {
            return new Date(x['@value']);
        } else if (t === 'xsd:base64Binary') {
            return Uint8Array.from(atob(x['@value']), (m) => m.codePointAt(0));
        }
    }
    return x;
}

async function sql(q, {parameters = [], headers = {'Accept': 'application/ld+json'}, auth = [], url = 'http://localhost:3803/sql'} = {}) {
    const body = new FormData();
    body.append('q', q);
    parameters.forEach(p => {
        body.append('parameter', JSON.stringify(jsonLDEncoder(p)));
    });

    if (auth.length == 2) {
        headers['Authorization'] = 'Basic ' + btoa(auth[0] + ":" + auth[1]);
    }

    const response = await fetch(url, {method: 'POST', headers: headers, body: body});

    if (response.ok) {
        const json = JSON.parse(await response.text(), (k, v) => jsonLDDecoder(v));
        if (json instanceof Object && json['@graph']) {
            return json['@graph'];
        } else {
            return json;
        }
    } else {
        throw new Error(response.status + ': ' + response.statusText + '\n' + await response.text());
    }
}

export { sql };

if (import.meta.url === `file://${process.argv[1]}`) {
    console.log(await sql(process.argv[2]));
}
