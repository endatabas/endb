/* global Module */

Module.onCustomMessage = (e) => {
    const { id, method, params } = e.data.userData;
    if (method === "common_lisp_eval") {
        const result = Module.ccall("common_lisp_eval", "string", ["string"], params);
        if (id) {
            window.postCustomMessage({ id, result });
        }
    }
};
Module.postRun = [() => {
    window.postCustomMessage({id: "postRun"});
}];
