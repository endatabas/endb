# Endatabas

## Building

Install https://www.quicklisp.org/ and https://www.sbcl.org/

On Ubuntu:

```bash
sudo apt install sbcl cl-quicklisp
sbcl --load /usr/share/common-lisp/source/quicklisp/quicklisp.lisp
```

On MacOS:

```bash
brew install sbcl
# For full instructions, see https://www.quicklisp.org/beta/#installation
curl -O https://beta.quicklisp.org/quicklisp.lisp
sbcl --load quicklisp.lisp
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

Initialise submodules:

```bash
git submodule update --init --recursive
```

Run the tests and build the binary:

```bash
make
```

## Sqllogictest

See https://www.sqlite.org/sqllogictest/

Install gcc or Clang which is needed to build the test runner.
On MacOS, Clang will be automatically installed. On Ubuntu:

```bash
sudo apt install build-essential libsqlite3-0
```

Build and run sanity checks. We patch the build slightly to inject our own database engine:

```bash
make slt-test
```

## Docker

Install https://podman.io/ or Docker:

```
sudo apt install podman
alias docker=podman
```

Build the image:

```bash
make docker
```

Run the image:

```
make run-docker
```


## Development Environment

See https://lispcookbook.github.io/cl-cookbook/getting-started.html for details.

## Copyright and License

Copyright 2023 Håkan Råberg and Steven Deobald.

Licensed under the GNU Affero General Public License v3.0.
