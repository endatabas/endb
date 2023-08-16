#!/usr/bin/env node

function jsonLDEncoder(x) {
    switch (x.constructor) {
        case Date:
            return {'@type': 'xsd:dateTime', '@value': x.toISOString()};
        case Uint8Array:
            const b = Array.from(x, (y) => String.fromCodePoint(y)).join('');
            return {'@type': 'xsd:base64Binary', '@value': btoa(b)};
        case Array:
            return x.map(jsonLDEncoder);
        case Object:
            return Object.fromEntries(Object.entries(x).map(([k, v], i) => [k, jsonLDEncoder(v)]));
        default:
            return x;
    }
}

function jsonLDDecoder(x) {
    if (Object.hasOwn(x, '@type') && Object.hasOwn(x, '@value')) {
        const t = x['@type'];
        if (t === 'xsd:date' || t === 'xsd:dateTime') {
            return new Date(x['@value']);
        } else if (t === 'xsd:base64Binary') {
            return Uint8Array.from(atob(x['@value']), (m) => m.codePointAt(0));
        }
    }
    if (Object.hasOwn(x, '@graph')) {
        return x['@graph'];
    }
    return x;
}

async function sql(q, {parameters = [], accept = 'application/ld+json', auth = [], url = 'http://localhost:3803/sql'} = {}) {
    const body = new FormData();

    body.append('q', q);
    parameters.forEach(p => {
        body.append('parameter', JSON.stringify(jsonLDEncoder(p)));
    });

    const headers = {'Accept': accept};

    if (auth.length == 2) {
        headers['Authorization'] = 'Basic ' + btoa(auth[0] + ":" + auth[1]);
    }

    const response = await fetch(url, {method: 'POST', headers: headers, body: body});

    if (response.ok) {
        return JSON.parse(await response.text(), (k, v) => jsonLDDecoder(v));
    } else {
        throw new Error(response.status + ': ' + response.statusText + '\n' + await response.text());
    }
}

export { sql };

if (import.meta.url === `file://${process.argv[1]}`) {
    console.log(await sql(process.argv[2]));
}
