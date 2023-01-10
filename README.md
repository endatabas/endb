# Endatabas

## Building

Install https://www.quicklisp.org/ and https://www.sbcl.org/

```bash
sudo apt install sbcl cl-quicklisp
sbcl --load /usr/share/common-lisp/source/quicklisp/quicklisp.lisp
```

Initialise Quicklisp:

```cl
(quicklisp-quickstart:install)
(ql:add-to-init-file)
```

Clone or link this project under `~/quicklisp/local-projects`:

```bash
ln -s $PWD ~/quicklisp/local-projects

```

Run the tests and build the binary:

```bash
make test endb
```

## Development Environment

See https://lispcookbook.github.io/cl-cookbook/getting-started.html for details.

## Copyright and License

Copyright 2023 Håkan Råberg and Steven Deobald.

Licensed under the GNU Affero General Public License v3.0.
