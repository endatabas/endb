.PHONY: doc

doc:
	rm -rf doc
	mkdir -p doc
	jsdoc2md -c jsdoc-conf.json -f endb.mjs --heading-depth 4 > doc/jsdoc.md
	cp doc/jsdoc.md ../../../endb-book/src/reference/jsdoc.md
