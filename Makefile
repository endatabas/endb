LISP ?= sbcl --noinform

SOURCES = $(shell find src -iname \*.lisp)
FASL_FILES = $(shell find . -iname \*.fasl)

CFLAGS = -g -Wall

SLT_SOURCES = sqllogictest.c md5.c sqlite3.c
SLT_ENGINE = CLSQLite

default: test

endb: Makefile *.asd $(SOURCES)
	$(LISP) --non-interactive \
		--eval '(ql:quickload :endb :silent t)' \
		--eval '(asdf:make :endb)'

repl:
	rlwrap $(LISP) --eval '(ql:quickload :endb :silent t)' --eval '(in-package :endb/core)'

run:
	$(LISP) --non-interactive --eval '(ql:quickload :endb :silent t)' --eval '(endb/core:main)'

run-binary: endb
	@./$<

test:
	$(LISP) --non-interactive \
		--eval '(ql:quickload :endb-test :silent t)' \
		--eval '(uiop:quit (if (fiveam:run-all-tests) 0 1))'

libsqllogictest.so: CFLAGS +=-DSQLITE_NO_SYNC=1 -DSQLITE_THREADSAFE=0 -DOMIT_ODBC=1 -shared -fPIC
libsqllogictest.so: Makefile sqllogictest/src/*
	cd sqllogictest/src && \
		sed -i s/int\ main/int\ sqllogictest_main/ sqllogictest.c && \
		$(CC) $(CFLAGS) -o $(CURDIR)/$@ $(SLT_SOURCES) && \
		git checkout .
	touch $@

slt-runner: Makefile *.asd slt/*.lisp libsqllogictest.so
	$(LISP) --non-interactive \
		--eval '(ql:quickload :endb-slt :silent t)' \
		--eval '(asdf:make :endb-slt)'
	touch $@

slt-sanity: slt-runner
	ls -1 sqllogictest/test/select* | xargs -i ./$< -engine $(SLT_ENGINE) -verify {}

clean:
	rm -f endb libsqllogictest.so slt-runner $(FASL_FILES)
	cd sqllogictest && git clean -f .

.PHONY: repl run run-binary test slt-sanity clean
