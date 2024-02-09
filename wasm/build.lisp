(ext:install-bytecodes-compiler)

(pushnew :wasm32 *features*)

(asdf:load-system :endb)

(require :cmp)
(ext:install-c-compiler)

(setf c::*cc* "emcc"
      c::*ld* "emcc"
      c::*ar* "emar"
      c::*ranlib* "emranlib"
      c::*user-cc-flags* "-Demscripten -I/root/ecl/ecl-emscripten/include"
      c::*user-linker-flags* "-L/root/ecl/ecl-emscripten/lib -lecl -sSTACK_SIZE=1048576 -lm"
      c::*ecl-include-directory* "/root/ecl/ecl-emscripten/include/ecl"
      c::*ecl-library-directory* "/root/ecl/ecl-emscripten/lib"
      c::*delete-files* nil
      c::*debug* 1
      cffi-sys::*cffi-ecl-method* :c/c++)

(labels ((walk (c)
           (unless (listp c)
             (let ((c (asdf:find-system c)))
               (append (loop for c in (asdf:component-sideway-dependencies c)
                             append (walk c))
                       (loop for c in (asdf/component:sub-components c)
                             when (typep c 'asdf:cl-source-file)
                               collect (namestring (asdf:component-pathname c))))))))
  (let ((lisp-files (set-difference (delete-duplicates (walk "endb") :test 'equal :from-end t)
                                    '("/root/endb/_build/fset/Code/testing.lisp"
                                      "/root/endb/_build/misc-extensions/src/tests.lisp"
                                      "/root/endb/_build/misc-extensions/src/context-tests.lisp")
                                    :test 'equal)))
    (format t "~%cross compiling ~A files:~%" (length lisp-files))
    (loop for c in lisp-files
          do (compile-file c :system-p t))

    (c:build-static-library "endb_lisp"
                            :lisp-files
                            (loop for c in lisp-files
                                  collect (concatenate 'string
                                                       (subseq c 0 (- (length c) (length "lisp")))
                                                       "o"))
                            :init-name "init_lib_endb_lisp")

    (quit)))
