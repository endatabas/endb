var Module = {
    print: (...text) => {
        var outputElement = document.getElementById('output');
        var inputElement = document.getElementById('input');
        var footerElement = document.getElementById('footer');
        text = text.join(' ');
        console.log(text);

        if (outputElement) {
            var div = document.createElement("div");
            div.innerText = text;
            outputElement.appendChild(div);
            footerElement.scrollIntoView();
        }
    },
    setStatus: (text) => {
        var statusElement = document.getElementById('status');
        var spinnerElement = document.getElementById('spinner');
        var inputElement = document.getElementById('input');
        var outputElement = document.getElementById('output');
        var footerElement = document.getElementById('footer');

        if (text === "") {
            spinnerElement.style.display = 'none';

            Module.endb_eval = Module.cwrap("endb_eval", "string", ["string"]);

            Module.endb_eval("(endb/lib:log-info \"version ~A\" (endb/lib:get-endb-version))");
            Module.endb_eval("(defvar *db* (endb/sql:begin-write-tx (endb/sql:make-db)))");

            console.log("running on https://ecl.common-lisp.dev/ powered by https://emscripten.org/")
            var div = document.createElement("div");
            div.innerHTML = "running on <a href=\"https://ecl.common-lisp.dev/\">https://ecl.common-lisp.dev/</a> powered by <a href=\"https://emscripten.org/\">https://emscripten.org/</a>";
            outputElement.appendChild(div);

            Module.print("print :help for help.\n\n");

            Module.sql = (sql) => {
                var json = Module.endb_eval(`
(endb/json:json-stringify
  (handler-case
      (multiple-value-bind (result result-code)
          (endb/sql:execute-sql *db* (endb/json:json-parse ${JSON.stringify(JSON.stringify(sql))}))
        (fset:map ("result" result) ("resultCode" result-code)))
    (error (e)
      (fset:map ("error" (format nil "~A" e))))))`);
                var {result, resultCode, error} = JSON.parse(JSON.parse(json));
                if (error) {
                    Module.print(error);
                } else {
                    if (!Array.isArray(resultCode)) {
                        result = [[resultCode]];
                        resultCode = ["result"];
                    }
                    console.log(resultCode.map((col) => JSON.stringify(col)).join("\t\t"));

                    var head = "<thead><tr>" + resultCode.map((col) => "<th>" + col + "</th>").join("") + "</tr></thead>";
                    var body = "<tbody>" + result.map((row) => {
                        console.log(row.map((col) => JSON.stringify(col)).join("\t\t"));
                        return "<tr>" + row.map((col) => "<td>" + JSON.stringify(col) + "</td>").join("") + "</tr>";
                    }).join("") + "</tbody>";

                    var table = document.createElement("table");
                    table.innerHTML = head + body;
                    outputElement.appendChild(table);
                }
                Module.print("\n");
            }

            function resizeInput() {
                inputElement.style.height = '';
                inputElement.style.height = inputElement.scrollHeight +'px'
                footerElement.scrollIntoView();
            }

            inputElement.addEventListener("input", (e) => {
                resizeInput();
            });
            inputElement.addEventListener("change", (e) => {
                resizeInput();
            });
            var commandHistory = [];
            var commandHistoryIndex = -1;

            function addToHistory(command) {
                if (commandHistory[commandHistory.length - 1] !== command) {
                    commandHistory.push(command);
                }
                commandHistoryIndex = commandHistory.length;
            }

            function resetInput(text) {
                inputElement.value = text;
                resizeInput();
            }

            inputElement.addEventListener("keydown", (e) => {
                if (e.keyCode == 13 && inputElement.value.trim() === ":help") {
                    var command = inputElement.value;
                    Module.print(command);
                    addToHistory(command);
                    resetInput("");
                    Module.print("key bindings:\nC-a\tbeginning of line\nC-e\tend of line\nC-RET\tevaluate\nM-p\tprevious history\n\M-n\tnext history\n\n");
                    e.preventDefault();
                } else if (e.keyCode == 13 && (e.ctrlKey || inputElement.value.trim().endsWith(";"))) {
                    var sql = inputElement.value;
                    addToHistory(sql);
                    resetInput("");
                    Module.print(sql);
                    Module.sql(sql);
                    e.preventDefault();
                } else if (e.ctrlKey) {
                    if (e.keyCode == 65) {
                        inputElement.setSelectionRange(0, 0);
                        e.preventDefault();
                    } else if (e.keyCode == 69) {
                        var end = inputElement.value.length;
                        inputElement.setSelectionRange(end, end);
                        e.preventDefault();
                    }
                } else if (e.altKey) {
                    if (e.keyCode == 78) {
                        if (commandHistoryIndex < commandHistory.length + 1) {
                            commandHistoryIndex++;
                            resetInput(commandHistory[commandHistoryIndex] ?? "");
                        }
                    } else if (e.keyCode == 80) {
                        if (commandHistoryIndex > 0) {
                            commandHistoryIndex--;
                            resetInput(commandHistory[commandHistoryIndex] ?? "");
                        }
                    }
                }
            });
            setTimeout(() => inputElement.focus(), 0);
        }
        statusElement.innerHTML = text;
    },
};
window.onerror = (event) => {
    var statusElement = document.getElementById('status');
    var spinnerElement = document.getElementById('spinner');
    Module.setStatus('Exception thrown, see JavaScript console');
    statusElement.style.display = 'block';
    spinnerElement.style.display = 'none';
    Module.setStatus = (text) => {
        if (text) console.error('[post-exception status] ' + text);
    };
};
