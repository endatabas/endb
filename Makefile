LISP ?= sbcl --noinform --dynamic-space-size 4096

SOURCES = $(shell find src -iname \*.lisp)
FASL_FILES = $(shell find . -iname \*.fasl)
SED_CMD = sed -i
SHARED_LIB_EXT = .so
ifeq ($(shell uname -s),Darwin)
	SED_CMD = sed -i.bak
	SHARED_LIB_EXT = .dylib
endif

CFLAGS = -g -Wall

CARGO = cargo

DOCKER_RUST_OS = bullseye
DOCKER_SBCL_OS = debian
DOCKER_ENDB_OS = debian
DOCKER_TAGS = -t endatabas/endb:$(DOCKER_ENDB_OS) -t endatabas/endb:latest-$(DOCKER_ENDB_OS) -t endatabas/endb:latest

LIB_PROFILE = release
LIB_PROFILE_DIR = $(LIB_PROFILE)
ifeq ($(LIB_PROFILE),dev)
	LIB_PROFILE_DIR = debug
endif

LIB_SOURCES = lib/Cargo.lock $(shell find lib/ -iname \*.toml) $(shell find lib/*/src -iname \*.rs)

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

TPCH_SF ?= 001
TPCH_REFERENCE_ENGINE = sqlite

default: test target/endb

target/endb: Makefile *.asd $(SOURCES) target/libendb$(SHARED_LIB_EXT)
	mkdir -p target
	$(LISP) --non-interactive \
		--eval '(ql:quickload :endb :silent t)' \
		--eval '(asdf:make :endb)'

repl:
	rlwrap $(LISP) --eval '(ql:quickload :endb :silent t)' --eval '(in-package :endb/core)'

run:
	$(LISP) --non-interactive --eval '(ql:quickload :endb :silent t)' --eval '(endb/core:main)'

run-binary: target/endb
	@rlwrap ./$<

test: lib-test target/libendb$(SHARED_LIB_EXT)
	$(LISP) --non-interactive \
		--eval '(ql:quickload :endb-test :silent t)' \
		--eval '(uiop:quit (if (fiveam:run-all-tests) 0 1))'

lib-check:
	(cd lib; $(CARGO) check)

lib-lint:
	(cd lib; $(CARGO) clippy)

lib-test: lib-lint
	(cd lib; $(CARGO) test)

lib-microbench:
	(cd lib; $(CARGO) run --profile $(LIB_PROFILE) --example micro_bench)

lib/target/$(LIB_PROFILE_DIR)/libendb$(SHARED_LIB_EXT): Makefile $(LIB_SOURCES)
	(cd lib; $(CARGO) build --profile $(LIB_PROFILE))

target/libendb$(SHARED_LIB_EXT): lib/target/$(LIB_PROFILE_DIR)/libendb$(SHARED_LIB_EXT)
	mkdir -p target
	cp $< $@ || true

target/sqllogictest_src: sqllogictest/src
	mkdir -p target
	rm -rf $@
	cp -a $< $@
	$(SED_CMD) s/int\ main/int\ sqllogictest_main/ $@/sqllogictest.c

target/libsqllogictest$(SHARED_LIB_EXT): CFLAGS += -DSQLITE_NO_SYNC=1 -DSQLITE_THREADSAFE=0 -DOMIT_ODBC=1 -shared -fPIC
target/libsqllogictest$(SHARED_LIB_EXT): Makefile target/sqllogictest_src
	cd target/sqllogictest_src && $(CC) $(CFLAGS) -o $(CURDIR)/$@ $(SLT_SOURCES)

target/slt: Makefile *.asd $(SOURCES) slt/*.lisp target/libsqllogictest$(SHARED_LIB_EXT) lib/target/$(LIB_PROFILE_DIR)/libendb$(SHARED_LIB_EXT)
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

TPCH_SCHEMA_FILE = test/tpch/tpch_schema.test
TPCH_QUERIES_FILE = test/tpch/$(TPCH_SF)/tpch_queries.test
TPCH_TABLE_FILES = test/tpch/$(TPCH_SF)/*_tbl.test.gz

target/tpch_$(TPCH_SF).test: $(TPCH_SCHEMA_FILE) $(TPCH_TABLE_FILES) $(TPCH_QUERIES_FILE)
	rm -f $@
	cat $(TPCH_SCHEMA_FILE) > $@
	cat $(TPCH_TABLE_FILES) | gunzip >> $@
	cat $(TPCH_QUERIES_FILE) >> $@

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
	docker build \
		--build-arg RUST_OS=$(DOCKER_RUST_OS) --build-arg SBCL_OS=$(DOCKER_SBCL_OS) --build-arg ENDB_OS=$(DOCKER_ENDB_OS) \
		$(DOCKER_TAGS) .

docker-alpine: DOCKER_RUST_OS = alpine
docker-alpine: DOCKER_SBCL_OS = alpine
docker-alpine: DOCKER_ENDB_OS = alpine
docker-alpine: DOCKER_TAGS = -t endatabas/endb:$(DOCKER_ENDB_OS) -t endatabas/endb:latest-$(DOCKER_ENDB_OS)
docker-alpine: docker

run-docker: docker
	docker run --rm -it endatabas/endb:latest-$(DOCKER_OS)

clean:
	(cd lib; $(CARGO) clean)
	rm -rf target $(FASL_FILES)

.PHONY: repl run run-binary test lib-check lib-lint lib-test lib-microbench \
	slt-test slt-test-select slt-test-random slt-test-index slt-test-evidence slt-test-all slt-test-tpch slt-test-ci \
	docker docker-alpine run-docker clean
