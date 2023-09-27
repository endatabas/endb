
;;  --load /usr/share/common-lisp/source/quicklisp/quicklisp.lisp --eval "(quicklisp-quickstart:install)" --eval "(ql-util:without-prompting)" --eval "(ql:add-to-init-file)"

(load "/usr/share/common-lisp/source/quicklisp/quicklisp.lisp")

(quicklisp-quickstart:install)
(ql-util:without-prompting)
(ql:add-to-init-file)
