LISP ?= sbcl

SOURCES := $(shell find . -iname \*.lisp)
FASL_FILES := $(shell find . -iname \*.fasl)

default: test

endb:	Makefile *.asd $(SOURCES)
	$(LISP) --noinform \
		--non-interactive \
		--eval '(ql:quickload :endb)' \
		--eval '(asdf:make :endb)'

repl:
	rlwrap $(LISP) --noinform --eval '(ql:quickload :endb)' --eval '(in-package :endb)'

run:
	$(LISP) --noinform --non-interactive --eval '(ql:quickload :endb)' --eval '(endb:main)'

run-binary: endb
	@./$<

test:
	$(LISP) --noinform \
		--non-interactive \
		--eval '(ql:quickload :endb/tests)' \
		--eval '(uiop:quit (if (fiveam:run-all-tests) 0 1))'

clean:
	rm -f endb $(FASL_FILES)

.PHONY: repl run run-binary test clean
