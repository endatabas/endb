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
SLT_ENGINE = endb
SLT_TESTS = sqllogictest/test/select1.test sqllogictest/test/select2.test sqllogictest/test/select3.test

default: test target/endb

target/endb: Makefile *.asd $(SOURCES)
	mkdir -p target
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

target/sqllogictest_src: sqllogictest/src
	mkdir -p target
	rm -rf $@
	cp -a $< $@
	$(SED_CMD) s/int\ main/int\ sqllogictest_main/ $@/sqllogictest.c

target/libsqllogictest$(SHARED_LIB_EXT): CFLAGS += -DSQLITE_NO_SYNC=1 -DSQLITE_THREADSAFE=0 -DOMIT_ODBC=1 -shared -fPIC
target/libsqllogictest$(SHARED_LIB_EXT): Makefile target/sqllogictest_src
	cd target/sqllogictest_src && $(CC) $(CFLAGS) -o $(CURDIR)/$@ $(SLT_SOURCES)

target/slt: Makefile *.asd $(SOURCES) slt/*.lisp target/libsqllogictest$(SHARED_LIB_EXT)
	$(LISP) --non-interactive \
		--eval '(ql:quickload :endb-slt :silent t)' \
		--eval '(asdf:make :endb-slt)'

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
