/* eslint-env worker */
/* global Module */

onmessage = (e) => {
    const { id, method, params } = e.data;
    if (method === "common_lisp_eval") {
        const result = Module.ccall("common_lisp_eval", "string", ["string"], params);
        if (id) {
            postMessage({ id, result });
        }
    }
};
Module.print = (...text) => {
    postMessage({method: "print", params: text})
};
Module.postRun = [() => {
    postMessage({method: "postRun", params: []});
}];
