LISP ?= sbcl --noinform

SOURCES := $(shell find src -iname \*.lisp)
FASL_FILES := $(shell find . -iname \*.fasl)

CC = gcc -g -Wall
CC += -DSQLITE_NO_SYNC=1 -DSQLITE_THREADSAFE=0 -rdynamic

SLT_ENGINE = CLSQLite

default: test

endb: Makefile *.asd $(SOURCES)
	$(LISP) --non-interactive \
		--eval '(ql:quickload :endb :silent t)' \
		--eval '(asdf:make :endb)'

repl:
	rlwrap $(LISP) --eval '(ql:quickload :endb :silent t)' --eval '(in-package :endb)'

run:
	$(LISP) --non-interactive --eval '(ql:quickload :endb :silent t)' --eval '(endb:main)'

run-binary: endb
	@./$<

test:
	$(LISP) --non-interactive \
		--eval '(ql:quickload :endb/tests :silent t)' \
		--eval '(uiop:quit (if (fiveam:run-all-tests) 0 1))'

libsqllogictest.so: Makefile sqllogictest/src/*.c sqllogictest/src/*.h
	cd sqllogictest/src && \
		echo 'void registerODBC3(void) {}' > slt_odbc3.c && \
		sed -i s/int\ main/int\ sqllogictest_main/ sqllogictest.c && \
		$(CC) -shared -fPIC -o $(CURDIR)/$@ sqllogictest.c md5.c sqlite3.c && \
		git checkout .
	touch $@

slt-runner: Makefile *.asd slt/endb-slt.lisp libsqllogictest.so
	$(LISP) --non-interactive \
		--eval '(ql:quickload :endb/slt :silent t)' \
		--eval '(asdf:make :endb/slt)'
	touch $@

slt-sanity: slt-runner
	ls -1 sqllogictest/test/select* | xargs -i ./$< -engine $(SLT_ENGINE) -verify {}

clean:
	rm -f endb libsqllogictest.so slt-runner $(FASL_FILES)
	cd sqllogictest && git clean -f .

.PHONY: repl run run-binary test slt-sanity clean
