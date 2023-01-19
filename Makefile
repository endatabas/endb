LISP ?= sbcl --noinform

SOURCES = $(shell find src -iname \*.lisp)
FASL_FILES = $(shell find . -iname \*.fasl)
SED_CMD = sed -i
SHARED_LIB_EXT = .so
ifeq ($(shell uname -s),Darwin)
	SED_CMD = sed -i.bak
	SHARED_LIB_EXT = .dylib
endif

CFLAGS = -g -Wall

SLT_SOURCES = sqllogictest.c md5.c sqlite3.c
SLT_ENGINE = CLSQLite
SLT_TESTS = $(shell ls -1 sqllogictest/test/select*)

default: test target/endb

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

target/libsqllogictest$(SHARED_LIB_EXT): CFLAGS += -DSQLITE_NO_SYNC=1 -DSQLITE_THREADSAFE=0 -DOMIT_ODBC=1 -shared -fPIC
target/libsqllogictest$(SHARED_LIB_EXT): target Makefile sqllogictest/src/*
	cd sqllogictest/src && \
		$(SED_CMD) s/int\ main/int\ sqllogictest_main/ sqllogictest.c && \
		$(CC) $(CFLAGS) -o $(CURDIR)/$@ $(SLT_SOURCES) && \
		$(SED_CMD) s/int\ sqllogictest_main/int\ main/ sqllogictest.c
	touch $@

target/slt: target Makefile *.asd $(SOURCES) slt/*.lisp target/libsqllogictest$(SHARED_LIB_EXT)
	$(LISP) --non-interactive \
		--eval '(ql:quickload :endb-slt :silent t)' \
		--eval '(asdf:make :endb-slt)'
	touch $@

slt-test: target/slt
	for test in $(SLT_TESTS); do ./$< -engine $(SLT_ENGINE) -verify $$test; done

slt-test-all: SLT_TESTS = $(shell find sqllogictest/test -iname *.test)
slt-test-all: slt-test

docker:
	docker build -t endatabas/endb:latest .

run-docker: docker
	docker run --rm -it endatabas/endb

clean:
	rm -rf target $(FASL_FILES)

.PHONY: repl run run-binary test slt-test slt-test-all docker run-docker clean
