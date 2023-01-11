LISP ?= sbcl --noinform

SOURCES := $(shell find . -iname \*.lisp)
FASL_FILES := $(shell find . -iname \*.fasl)

CC = gcc -g -Wall
CC += -DSQLITE_NO_SYNC=1 -DSQLITE_THREADSAFE=0 -rdynamic

default: test

endb: Makefile *.asd $(SOURCES)
	$(LISP) --non-interactive \
		--eval '(ql:quickload :endb)' \
		--eval '(asdf:make :endb)'

repl:
	rlwrap $(LISP) --eval '(ql:quickload :endb)' --eval '(in-package :endb)'

run:
	$(LISP) --non-interactive --eval '(ql:quickload :endb :silent t)' --eval '(endb:main)'

run-binary: endb
	@./$<

test:
	$(LISP) --non-interactive \
		--eval '(ql:quickload :endb/tests :silent t)' \
		--eval '(uiop:quit (if (fiveam:run-all-tests) 0 1))'

libslt_endb.so: slt/slt_endb.c
	gcc -g -Wall -shared -fpic -Isqllogictest/src $< -o $@

sqllogictest/src/sqllogictest: Makefile libslt_endb.so
	echo 'extern void registerODBC3(void);' > sqllogictest/src/slt_odbc3.c
	cd sqllogictest/src; make md5.o sqlite3.o; $(CC) -o sqllogictest sqllogictest.c md5.o sqlite3.o -L$(CURDIR) -lslt_endb; git checkout .

sqllogictest/src/libsqllogictest.so: Makefile libslt_endb.so
	echo 'void registerODBC3(void) {}' > sqllogictest/src/slt_odbc3.c
	cd sqllogictest/src; sed -i s/int\ main/int\ sltmain/ sqllogictest.c ; $(CC) -shared -fPIC -o libsqllogictest.so sqllogictest.c md5.c sqlite3.c -L$(CURDIR) -lslt_endb; git checkout .

slt-sanity: sqllogictest/src/sqllogictest
	cd sqllogictest/src; ls -1 ../test/select* | LD_LIBRARY_PATH=$(CURDIR) xargs -i ./sqllogictest --verify {}

slt-sanity.so: sqllogictest/src/libsqllogictest.so
	ls -1 sqllogictest/test/select* | LD_LIBRARY_PATH=sqllogictest/src xargs -i $(LISP) --non-interactive --eval '(ql:quickload :endb/slt :silent t)' --eval '(endb-slt:slt-main)' --verify {}

clean:
	rm -f endb $(FASL_FILES) libslt_endb.so
	cd sqllogictest; git clean -f .

.PHONY: repl run run-binary test slt-sanity clean
