LISP ?= sbcl --noinform

SOURCES := $(shell find src -iname \*.lisp)
FASL_FILES := $(shell find . -iname \*.fasl)

CC = gcc -g -Wall
CC += -DSQLITE_NO_SYNC=1 -DSQLITE_THREADSAFE=0 -rdynamic

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

sqllogictest/src/libsqllogictest.so: Makefile
	echo 'void registerODBC3(void) {}' > sqllogictest/src/slt_odbc3.c
	cd sqllogictest/src; sed -i s/int\ main/int\ sltmain/ sqllogictest.c ; $(CC) -shared -fPIC -o libsqllogictest.so sqllogictest.c md5.c sqlite3.c; git checkout .

slt-runner: Makefile *.asd slt/endb-slt.lisp sqllogictest/src/libsqllogictest.so
	$(LISP) --non-interactive \
		--eval '(ql:quickload :endb/slt :silent t)' \
		--eval '(asdf:make :endb/slt)'

slt-sanity: slt-runner
	ls -1 sqllogictest/test/select* | LD_LIBRARY_PATH=sqllogictest/src xargs -i ./slt-runner --verify {}

clean:
	rm -f endb slt-runner $(FASL_FILES)
	cd sqllogictest; git clean -f .

.PHONY: repl run run-binary test slt-sanity clean
