var Module = {
    print: (...text) => {
        var outputElement = document.getElementById('output');
        var inputElement = document.getElementById('input');
        text = text.join(' ');
        console.log(text);

        if (outputElement) {
            outputElement.innerText += text + "\n";
            inputElement.scrollIntoView();
        }
    },
    setStatus: (text) => {
        var statusElement = document.getElementById('status');
        var spinnerElement = document.getElementById('spinner');
        var inputElement = document.getElementById('input');

        if (text === "") {
            spinnerElement.style.display = 'none';
            Module.endb_eval = Module.cwrap("endb_eval", "string", ["string"]);
            Module.endb_eval("(endb/lib:log-info \"version ~A\" (endb/lib:get-endb-version))");
            Module.endb_eval("(defvar *db* (endb/sql:begin-write-tx (endb/sql:make-db)))");
            Module.print("powered by https://emscripten.org/ running on https://ecl.common-lisp.dev/")
            Module.print("print :help for help.\n");
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
                    Module.print(resultCode.join("\t\t"));
                    result.forEach((row) => {
                        Module.print(row.join("\t\t"));
                    });
                }
            }

            function resizeInput() {
                inputElement.style.height = '';
                inputElement.style.height = inputElement.scrollHeight +'px'
            }

            inputElement.addEventListener("input", (e) => {
                resizeInput();
            });
            inputElement.addEventListener("change", (e) => {
                resizeInput();
            });
            var commandHistory = [];
            var commandHistoryIndex = -1;
            inputElement.addEventListener("keydown", (e) => {
                if (e.keyCode == 13 && inputElement.value.trim() === ":help") {
                    var command = inputElement.value;
                    Module.print(command);
                    commandHistory.push(command);
                    commandHistoryIndex = commandHistory.length;
                    inputElement.value = '';
                    Module.print("key bindings:\nC-a\tbeginning of line\nC-e\tend of line\nC-RET\tevaluate\nM-p\tprevious history\n\M-n\tnext history\n");
                    e.preventDefault();
                } else if (e.keyCode == 13 && (e.ctrlKey || inputElement.value.trim().endsWith(";"))) {
                    var sql = inputElement.value;
                    commandHistory.push(sql);
                    commandHistoryIndex = commandHistory.length;
                    inputElement.value = '';
                    Module.print(sql);
                    Module.sql(sql);
                    Module.print("");
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
                            inputElement.value = commandHistory[commandHistoryIndex] ?? "";
                        }
                    } else if (e.keyCode == 80) {
                        if (commandHistoryIndex > 0) {
                            commandHistoryIndex--;
                            inputElement.value = commandHistory[commandHistoryIndex] ?? "";
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
    Module.setStatus('Exception thrown, see JavaScript console');
    statusElement.style.display = 'block';
    spinnerElement.style.display = 'none';
    Module.setStatus = (text) => {
        if (text) console.error('[post-exception status] ' + text);
    };
};
