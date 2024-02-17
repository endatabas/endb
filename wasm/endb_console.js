/* eslint-env browser */

let worker;
const sentMessages = {};

let id = 0;
async function sendToWorker(method, params) {
    id++;
    let resolve, reject;
    const promise = new Promise((res, rej) => {
        resolve = res;
        reject = rej;
    });
    sentMessages[id.toString()] = { resolve, reject };
    const message = {id: id.toString(), method, params};
    worker.postMessage(message);

    return promise;
}

const spinnerElement = document.getElementById("spinner");
const statusElement = document.getElementById("spinner");
const inputElement = document.getElementById("input");
const outputElement = document.getElementById("output");
const footerElement = document.getElementById("footer");

const EndbConsole = {
    print: (...text) => {
        text = text.join(" ");
        console.log(text);

        const div = document.createElement("div");
        div.innerText = text;
        outputElement.appendChild(div);
        footerElement.scrollIntoView({block: "nearest"});
    },
    postRun: async () => {
        spinnerElement.style.display = "none";
        inputElement.style.display = "block";

        await sendToWorker("common_lisp_eval", [`
(progn
  (endb/lib:log-info "version ~A" (endb/lib:get-endb-version))
  (endb/lib:log-info "data directory :memory:")
  (defvar *db* (endb/sql:make-db)))`]);

        console.log("running on https://ecl.common-lisp.dev/ powered by https://emscripten.org/");
        const div = document.createElement("div");
        div.innerHTML = "running on <a href=\"https://ecl.common-lisp.dev/\" target=\"_top\">https://ecl.common-lisp.dev/</a> powered by <a href=\"https://emscripten.org/\" target=\"_top\">https://emscripten.org/</a>";
        outputElement.appendChild(div);

        EndbConsole.print("print :help for help.\n\n");

        async function executeSQL(sql) {
            const json = await sendToWorker("common_lisp_eval", [`
(let ((endb/json:*json-ld-scalars* nil))
  (endb/json:json-stringify
    (handler-case
        (let ((write-db (endb/sql:begin-write-tx *db*)))
          (multiple-value-bind (result result-code)
              (endb/sql:execute-sql write-db (endb/json:json-parse ${JSON.stringify(JSON.stringify(sql))}))
            (setf *db* (endb/sql:commit-write-tx *db* write-db))
            (fset:map ("result" result) ("resultCode" result-code))))
      (error (e)
        (fset:map ("error" (format nil "~A" e)))))))`]);
            let {result, resultCode, error} = JSON.parse(JSON.parse(json));
            if (error) {
                EndbConsole.print(error);
            } else {
                if (!Array.isArray(resultCode)) {
                    result = [[resultCode]];
                    resultCode = ["result"];
                }

                console.log(resultCode.join("\t\t"));
                result.forEach((row) => {
                    console.log(row.map((col) => JSON.stringify(col)).join("\t\t"));
                });

                const table = document.createElement("table");

                {
                    const thead = document.createElement("thead");
                    const tr = document.createElement("tr");
                    resultCode.forEach((col) => {
                        const th = document.createElement("th");
                        th.innerText = col;
                        tr.appendChild(th);
                    })
                    thead.appendChild(tr);
                    table.appendChild(thead);
                }

                {
                    const tbody = document.createElement("tbody");
                    result.forEach((row) => {
                        const tr = document.createElement("tr");
                        row.forEach((col) => {
                            const td = document.createElement("td");
                            td.innerText = JSON.stringify(col);
                            tr.appendChild(td);
                        });
                        tbody.appendChild(tr);
                    });
                    table.appendChild(tbody);
                }

                outputElement.appendChild(table);
            }
            EndbConsole.print("\n");
        }

        function resizeInput() {
            inputElement.style.height = "";
            inputElement.style.height = inputElement.scrollHeight +"px"
            footerElement.scrollIntoView({block: "nearest"});
        }

        window.addEventListener("resize", () => {
            resizeInput();
        });
        inputElement.addEventListener("input", () => {
            resizeInput();
        });
        inputElement.addEventListener("change", () => {
            resizeInput();
        });

        function resetInput(text) {
            inputElement.value = text;
            resizeInput();
        }

        let commandHistory = [];
        let commandHistoryIndex = -1;

        function addToHistory(command) {
            if (commandHistory[commandHistory.length - 1] !== command) {
                commandHistory.push(command);
            }
            commandHistoryIndex = commandHistory.length;
        }

        inputElement.addEventListener("keydown", (e) => {
            if (e.keyCode == 13 && inputElement.value.trim().startsWith(":")) {
                let command = inputElement.value;
                addToHistory(command);
                resetInput("");
                EndbConsole.print(command);
                command = command.trim();
                if (command === ":help") {
                    EndbConsole.print("key bindings:\nC-a\tbeginning of line\nC-e\tend of line\nC-RET\tevaluate\nM-p\tprevious history\nM-n\tnext history\n\n");
                } else {
                    EndbConsole.print(`unknown command ${command}\n\n`);
                }
                e.preventDefault();
            } else if (e.keyCode == 13 && (e.ctrlKey || inputElement.value.trim().endsWith(";"))) {
                const sql = inputElement.value;
                addToHistory(sql);
                resetInput("");
                EndbConsole.print(sql);
                executeSQL(sql);
                e.preventDefault();
            } else if (e.ctrlKey) {
                if (e.keyCode == 65) {
                    inputElement.setSelectionRange(0, 0);
                    e.preventDefault();
                } else if (e.keyCode == 69) {
                    const end = inputElement.value.length;
                    inputElement.setSelectionRange(end, end);
                    e.preventDefault();
                }
            } else if (e.altKey) {
                if (e.keyCode == 78) {
                    if (commandHistoryIndex < commandHistory.length + 1) {
                        commandHistoryIndex++;
                        resetInput(commandHistory[commandHistoryIndex] ?? "");
                    }
                    e.preventDefault();
                } else if (e.keyCode == 80) {
                    if (commandHistoryIndex > 0) {
                        commandHistoryIndex--;
                        resetInput(commandHistory[commandHistoryIndex] ?? "");
                    }
                    e.preventDefault();
                }
            }
        });
        setTimeout(() => inputElement.focus(), 0);
    }
}

window.addEventListener("error", () => {
    statusElement.innerHTML = "Exception thrown, see JavaScript console";
    statusElement.style.display = "block";
    spinnerElement.style.display = "none";
});

worker = new Worker("endb.js");
worker.onmessage = (e) => {
    const { id, method, params, result } = e.data;
    if (method) {
        const fn = EndbConsole[method];
        if (fn) {
            const result = fn.apply(null, params);
            if (id) {
                worker.postMessage({id, result});
            }
        }
    } else if (id) {
        const promise = sentMessages[id];
        delete sentMessages[id];
        if (promise) {
            promise.resolve(result);
        }
    }
}
