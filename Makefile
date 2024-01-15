SBCL_DYNAMIC_SPACE_SIZE ?= 8192
LISP ?= sbcl --noinform --dynamic-space-size $(SBCL_DYNAMIC_SPACE_SIZE) --no-userinit --no-sysinit --load _build/setup.lisp

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

DOCKER = docker
DOCKER_RUST_OS = bullseye
DOCKER_SBCL_OS = debian
DOCKER_ENDB_OS = debian
DOCKER_TAGS = -t endatabas/endb:$(DOCKER_ENDB_OS) -t endatabas/endb:latest-$(DOCKER_ENDB_OS) -t endatabas/endb:latest
ifeq ($(shell docker --version | cut -d" " -f1),podman)
	DOCKER_IMAGE = localhost/endatabas/endb:latest
  DOCKER_PULL_ALWAYS = -always
else
	DOCKER_IMAGE = endatabas/endb:latest
  DOCKER_PULL_ALWAYS = =always
endif
DOCKER_ID = $(shell docker images -q $(DOCKER_IMAGE))
PODMAN_USR = $(shell grep -sq "^unqualified-search-registries = \[\"docker.io\"\]" /etc/containers/registries.conf || grep -sq "^unqualified-search-registries = \[\"docker.io\"\]" ~/.config/containers/registries.conf)

LIB_PROFILE = release
LIB_PROFILE_DIR = $(LIB_PROFILE)
ifeq ($(LIB_PROFILE),dev)
	LIB_PROFILE_DIR = debug
endif

LIB_SOURCES = lib/Cargo.lock $(shell find lib/ -iname \*.toml) $(shell find lib/*/src -iname \*.rs) $(shell find lib/ -iname build.rs)

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

default: test target/endb

endb_data:
	mkdir -p endb_data

target/endb: Makefile *.asd $(SOURCES) target/libendb$(SHARED_LIB_EXT)
	mkdir -p target
	$(LISP) --non-interactive \
		--eval '(asdf:load-system :endb)' \
		--eval '(asdf:make :endb)'

repl:
	rlwrap $(LISP) --eval '(asdf:load-system :endb)' --eval '(in-package :endb/core)'

run:
	$(LISP) --non-interactive --eval '(asdf:load-system :endb)' --eval '(endb/core:main)'

run-binary: target/endb
	@rlwrap ./$<

test: lib-test target/libendb$(SHARED_LIB_EXT)
	$(LISP) --non-interactive \
		--eval '(asdf:load-system :endb-test)' \
		--eval '(uiop:quit (if (fiveam:run-all-tests) 0 1))'

check: test slt-test-expr slt-test-sql-acid slt-test-tpch

update-submodules:
	git submodule update --init --recursive --force --jobs 4

lib-check:
	(cd lib; $(CARGO) check)

lib-lint:
	(cd lib; $(CARGO) clippy)

lib-update:
	(cd lib; $(CARGO) update)

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

target/libsqllogictest$(SHARED_LIB_EXT): CFLAGS += -DSQLITE_NO_SYNC=1 -DSQLITE_THREADSAFE=0 -DOMIT_ODBC=1 -DSQLITE_ENABLE_MATH_FUNCTIONS=1 -shared -fPIC
target/libsqllogictest$(SHARED_LIB_EXT): Makefile target/sqllogictest_src
	cd target/sqllogictest_src && $(CC) $(CFLAGS) -o $(CURDIR)/$@ $(SLT_SOURCES)

target/slt: Makefile *.asd $(SOURCES) slt/*.lisp target/libsqllogictest$(SHARED_LIB_EXT) lib/target/$(LIB_PROFILE_DIR)/libendb$(SHARED_LIB_EXT)
	$(LISP) --non-interactive \
		--eval '(asdf:load-system :endb-slt)' \
		--eval '(asdf:make :endb-slt)'

slt-test: target/slt
	for test in $(SLT_TESTS); do $(SLT_ENV) ./$< --engine $(SLT_ENGINE) $(SLT_ARGS) $$test; done

slt-test-select: SLT_TESTS = $(SLT_SELECT_TESTS)
slt-test-select: slt-test

slt-test-random: SLT_TESTS = $(SLT_RANDOM_TESTS)
slt-test-random: SLT_ENV += SB_INTERPRET=1
slt-test-random: slt-test

slt-test-index: SLT_TESTS = $(SLT_INDEX_TESTS)
slt-test-index: SLT_ENV += SB_INTERPRET=1
slt-test-index: slt-test

slt-test-evidence: SLT_TESTS = $(SLT_EVIDENCE_TESTS)
slt-test-evidence: SLT_ENV += ENDB_ENGINE_REPORTED_NAME=sqlite
slt-test-evidence: slt-test

slt-test-all: SLT_TESTS = $(shell find sqllogictest/test -iname *.test | grep -v evidence)
slt-test-all: slt-test

TPCH_SCHEMA_FILE = slt/tpch/tpch_schema.test
TPCH_QUERIES_FILE = slt/tpch/$(TPCH_SF)/tpch_queries.test
TPCH_TABLE_FILES = slt/tpch/$(TPCH_SF)/*_tbl.test.gz

target/tpch_$(TPCH_SF).test: $(TPCH_SCHEMA_FILE) $(TPCH_TABLE_FILES) $(TPCH_QUERIES_FILE)
	rm -f $@
	cat $(TPCH_SCHEMA_FILE) > $@
	cat $(TPCH_TABLE_FILES) | gunzip >> $@
	cat $(TPCH_QUERIES_FILE) >> $@

target/tpch_$(TPCH_SF)_sqlite.test: target/slt target/tpch_$(TPCH_SF).test
	rm target/tpch_$(TPCH_SF).db
	SLT_TIMING=0 SB_SPROF=0 ./target/slt -e sqlite --connection target/tpch_$(TPCH_SF).db target/tpch_$(TPCH_SF).test > $@

slt-test-tpch: SLT_TESTS = target/tpch_$(TPCH_SF).test
slt-test-tpch: SLT_ENV += ENDB_ENGINE_REPORTED_NAME=endb
slt-test-tpch: target/slt target/tpch_$(TPCH_SF)_sqlite.test
	$(SLT_ENV) ./target/slt -e $(SLT_ENGINE) $(SLT_ARGS) target/tpch_$(TPCH_SF)_sqlite.test

target/%_sqlite.test: slt/%.test target/slt
	SLT_TIMING=0 SB_SPROF=0 ./target/slt -e sqlite --connection :memory: $< > $@

slt-test-expr: target/expr_sqlite.test
	$(SLT_ENV) ./target/slt -e $(SLT_ENGINE) $(SLT_ARGS) $<

slt-test-ci: SLT_ENV += SLT_TIMING=1
slt-test-ci:
	$(SLT_ENV) make slt-test-expr
	$(SLT_ENV) make slt-test-sql-acid
	$(SLT_ENV) make slt-test-select
	$(SLT_ENV) make slt-test-evidence
	$(SLT_ENV) make slt-test-random
	$(SLT_ENV) make slt-test-index
	$(SLT_ENV) make slt-test-tpch

SQL_ACID_TEST_DIR = sqlacidtest/
SQL_ACID_TESTS = $(shell find $(SQL_ACID_TEST_DIR) -iwholename "*/tests/*/*.sql" | xargs -i basename {} | sort)

SLT_SQL_ACID_TESTS_SKIP = test005.sql test006.sql test007.sql test017.sql test020.sql

target/sql_acid.test: SHELL = /bin/bash
target/sql_acid.test:
		for test in $(SQL_ACID_TESTS); \
			do \
				echo "$(SLT_SQL_ACID_TESTS_SKIP)" | grep -q "$$test" && echo "skipif endb" >> $@; \
				echo "query T nosort $$test" >> $@; \
				find $(SQL_ACID_TEST_DIR) -iname $$test | xargs -i sh -c 'cat {} | grep -vE "^(--.*)$$" | grep -vE "^\s*$$"' >> $@; \
				echo -e "----\nT\n" >> $@; \
		done;

slt-test-sql-acid: SLT_ENV += ENDB_ENGINE_REPORTED_NAME=endb
slt-test-sql-acid: target/sql_acid.test target/slt
	$(SLT_ENV) ./target/slt -e $(SLT_ENGINE) $(SLT_ARGS) $<

sql-acid-test: target/endb
	ENDB_PID=$$(./$< -d :memory: > target/endb_sql_acid_test.log 2>&1 & echo $$!); \
		for test in $(SQL_ACID_TESTS); \
			do find $(SQL_ACID_TEST_DIR) -iname $$test | xargs -i examples/endb_console.py {}; \
		done; \
		kill $$ENDB_PID

TPCC_SF ?= 1
TPCC_ARGS += --config=$(shell realpath target/tpcc_$(TPCC_SF).config) endb --stop-on-error --scalefactor $(TPCC_SF)

target/tpcc_$(TPCC_SF).config:
	./tpcc/tpcc.sh --print-config endb > $@

target/tpcc_$(TPCC_SF)_data_load: TPCC_ARGS += -no-execute
target/tpcc_$(TPCC_SF)_data_load: target/endb target/tpcc_$(TPCC_SF).config
	ENDB_PID=$$(./$< -d $@ > target/tpcc_$(TPCC_SF)_load.log 2>&1 & echo $$!); \
		./tpcc/tpcc.sh $(TPCC_ARGS); \
		kill $$ENDB_PID

tpcc: TPCC_ARGS += --no-load
tpcc:  target/endb target/tpcc_$(TPCC_SF)_data_load target/tpcc_$(TPCC_SF).config
	rm -rf target/tpcc_$(TPCC_SF)_data
	cp -a target/tpcc_$(TPCC_SF)_data_load target/tpcc_$(TPCC_SF)_data
	ENDB_PID=$$(./$< -d target/tpcc_$(TPCC_SF)_data > target/tpcc_$(TPCC_SF)_execute.log 2>&1 & echo $$!); sleep 3; \
		./tpcc/tpcc.sh $(TPCC_ARGS); \
		kill $$ENDB_PID

docker:
	$(DOCKER) build --pull$(DOCKER_PULL_ALWAYS) \
		--build-arg ENDB_GIT_DESCRIBE=$(shell git describe --always --dirty) \
		--build-arg RUST_OS=$(DOCKER_RUST_OS) --build-arg SBCL_OS=$(DOCKER_SBCL_OS) --build-arg ENDB_OS=$(DOCKER_ENDB_OS) \
		$(DOCKER_TAGS) .

docker-alpine: DOCKER_RUST_OS = alpine
docker-alpine: DOCKER_SBCL_OS = alpine
docker-alpine: DOCKER_ENDB_OS = alpine
docker-alpine: DOCKER_TAGS = -t endatabas/endb:$(DOCKER_ENDB_OS) -t endatabas/endb:latest-$(DOCKER_ENDB_OS)
docker-alpine: docker

run-docker: docker endb_data
	$(DOCKER) run --rm -p 3803:3803 -v "$(PWD)/endb_data":/app/endb_data -it endatabas/endb:latest-$(DOCKER_ENDB_OS)

# explicit builds mean `push-docker` does not depend on build directly
push-docker:
ifeq ($(PODMAN_USR),)
	@echo "\nWARNING: 'unqualified-search-registries' is missing. Looked in:"
	@echo "    /etc/containers/registries.conf"
	@echo "    ~/.config/containers/registries.conf\n"
	@echo "Remove this warning by running the following command:"
	@echo "echo 'unqualified-search-registries = [\"docker.io\"]' >> ~/.config/containers/registries.conf\n"
	@echo "'push-docker' target failed."
else
	@echo "'unqualified-search-registries' successfully detected."
	$(DOCKER) login --username=endatabas
	$(DOCKER) tag $(DOCKER_ID) endatabas/endb:latest
	$(DOCKER) push endatabas/endb:latest
endif

clean:
	(cd lib; $(CARGO) clean)
	rm -rf target $(FASL_FILES)

.PHONY: repl run run-binary test check lib-check lib-lint lib-update lib-test lib-microbench update-submodules \
	slt-test slt-test-select slt-test-random slt-test-index slt-test-evidence slt-test-all slt-test-tpch slt-test-expr slt-test-ci \
	slt-test-sql-acid sql-acid-test tpcc docker docker-alpine run-docker push-docker clean
