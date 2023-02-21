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
SLT_ARGS = --verify
SLT_ENV =

SLT_SELECT_TESTS = $(shell ls -1 sqllogictest/test/select*.test)
SLT_RANDOM_TESTS = $(shell ls -1 sqllogictest/test/random/*/slt_good_0.test)
SLT_INDEX_TESTS = $(shell ls -1 sqllogictest/test/index/*/10/slt_good_0.test)
SLT_EVIDENCE_TESTS = sqllogictest/test/evidence/in1.test \
	sqllogictest/test/evidence/in2.test \
	sqllogictest/test/evidence/slt_lang_aggfunc.test \
	sqllogictest/test/evidence/slt_lang_createview.test \
	sqllogictest/test/evidence/slt_lang_droptable.test \
	sqllogictest/test/evidence/slt_lang_dropview.test \
	sqllogictest/test/evidence/slt_lang_update.test

SLT_TESTS = $(SLT_SELECT_TESTS)

TPCH_SF = 001
TPCH_REFERENCE_ENGINE = sqlite

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
	for test in $(SLT_TESTS); do $(SLT_ENV) ./$< --engine $(SLT_ENGINE) $(SLT_ARGS) $$test; done

slt-test-select: SLT_TESTS = $(SLT_SELECT_TESTS)
slt-test-select: slt-test

slt-test-random: SLT_TESTS = $(SLT_RANDOM_TESTS)
slt-test-random: slt-test

slt-test-index: SLT_TESTS = $(SLT_INDEX_TESTS)
slt-test-index: SLT_ENV += SB_INTERPRET=1
slt-test-index: slt-test

slt-test-evidence: SLT_TESTS = $(SLT_EVIDENCE_TESTS)
slt-test-evidence: SLT_ENV += ENDB_ENGINE_REPORTED_NAME=sqlite
slt-test-evidence: slt-test

slt-test-all: SLT_TESTS = $(shell find sqllogictest/test -iname *.test | grep -v evidence)
slt-test-all: SLT_ENV += SB_INTERPRET=1
slt-test-all: slt-test

target/tpch_$(TPCH_SF).test: test/tpch/tpch_schema.test test/tpch/$(TPCH_SF)/*_tbl.test test/tpch/$(TPCH_SF)/tpch_queries.test
	rm -f $@
	cat $^ > $@

slt-test-tpch: SLT_TESTS = target/tpch_$(TPCH_SF).test
slt-test-tpch: SLT_ENV += ENDB_ENGINE_REPORTED_NAME=endb
slt-test-tpch: target/slt target/tpch_$(TPCH_SF).test
	SLT_TIMING=0 ./target/slt -e $(TPCH_REFERENCE_ENGINE) target/tpch_$(TPCH_SF).test > target/tpch_$(TPCH_SF)_sqlite.test
	$(SLT_ENV) ./target/slt -e $(SLT_ENGINE) $(SLT_ARGS) target/tpch_$(TPCH_SF)_sqlite.test

slt-test-ci: SLT_ENV += SLT_TIMING=1
slt-test-ci:
	$(SLT_ENV) make slt-test-select
	$(SLT_ENV) make slt-test-evidence
	$(SLT_ENV) make slt-test-random
	$(SLT_ENV) make slt-test-index
	$(SLT_ENV) make slt-test-tpch

docker:
	docker build -t endatabas/endb:latest .

run-docker: docker
	docker run --rm -it endatabas/endb

clean:
	rm -rf target $(FASL_FILES)

.PHONY: repl run run-binary test slt-test slt-test-select slt-test-random slt-test-index slt-test-evidence slt-test-all slt-test-tpch slt-test-ci docker run-docker clean
