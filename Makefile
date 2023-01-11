LISP ?= sbcl

SOURCES := $(shell find . -iname \*.lisp)
FASL_FILES := $(shell find . -iname \*.fasl)

default: test

endb: Makefile *.asd $(SOURCES)
	$(LISP) --noinform \
		--non-interactive \
		--eval '(ql:quickload :endb)' \
		--eval '(asdf:make :endb)'

repl:
	rlwrap $(LISP) --noinform --eval '(ql:quickload :endb)' --eval '(in-package :endb)'

run:
	$(LISP) --noinform --non-interactive --eval '(ql:quickload :endb)' --eval '(endb:main)'

run-binary: endb
	@./$<

test:
	$(LISP) --noinform \
		--non-interactive \
		--eval '(ql:quickload :endb/tests)' \
		--eval '(uiop:quit (if (fiveam:run-all-tests) 0 1))'

sqllogictest/src/sqllogictest: Makefile
	echo 'void registerODBC3(void){ printf("Registering ENDB engine.\\n"); }' > sqllogictest/src/slt_odbc3.c
	cd sqllogictest/src; make; git checkout .

slt-sanity: sqllogictest/src/sqllogictest
	cd sqllogictest/src; ls -1 ../test/select* | xargs -i ./sqllogictest --verify {}

clean:
	rm -f endb $(FASL_FILES)
	cd sqllogictest; git clean -f .

.PHONY: repl run run-binary test slt-sanity clean
