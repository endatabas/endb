LISP ?= sbcl --noinform

SOURCES = $(shell find src -iname \*.lisp)
FASL_FILES = $(shell find . -iname \*.fasl)

CFLAGS = -g -Wall

SLT_SOURCES = sqllogictest.c md5.c sqlite3.c
SLT_ENGINE = CLSQLite

default: test

target:
	mkdir target

target/endb: target Makefile *.asd $(SOURCES)
	$(LISP) --non-interactive \
		--eval '(ql:quickload :endb :silent t)' \
		--eval '(asdf:make :endb)'

repl:
	rlwrap $(LISP) --eval '(ql:quickload :endb :silent t)' --eval '(in-package :endb/core)'

run:
	$(LISP) --non-interactive --eval '(ql:quickload :endb :silent t)' --eval '(endb/core:main)'

run-binary: target/endb
	@./$<

test:
	$(LISP) --non-interactive \
		--eval '(ql:quickload :endb-test :silent t)' \
		--eval '(uiop:quit (if (fiveam:run-all-tests) 0 1))'

target/libsqllogictest.so: CFLAGS +=-DSQLITE_NO_SYNC=1 -DSQLITE_THREADSAFE=0 -DOMIT_ODBC=1 -shared -fPIC
target/libsqllogictest.so: target Makefile sqllogictest/src/*
	cd sqllogictest/src && \
		sed -i s/int\ main/int\ sqllogictest_main/ sqllogictest.c && \
		$(CC) $(CFLAGS) -o $(CURDIR)/$@ $(SLT_SOURCES) && \
		which git && git checkout . || true
	touch $@

target/slt: target Makefile *.asd slt/*.lisp target/libsqllogictest.so
	$(LISP) --non-interactive \
		--eval '(ql:quickload :endb-slt :silent t)' \
		--eval '(asdf:make :endb-slt)'
	touch $@

slt-sanity: target/slt
	ls -1 sqllogictest/test/select* | xargs -i ./$< -engine $(SLT_ENGINE) -verify {}

docker:
	docker build -t endatabas/endb:latest .

run-docker: docker
	docker run --rm -it endatabas/endb

clean:
	rm -rf target $(FASL_FILES)
	cd sqllogictest && git clean -f .

.PHONY: repl run run-binary test slt-sanity qlot-repl docker run-docker clean
