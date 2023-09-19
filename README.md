![Endatabas](https://www.endatabas.com/resources/images/github-banner-logo_3200x476.png)

[![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)](https://hub.docker.com/r/endatabas/endb)
[![Stack Overflow](https://img.shields.io/badge/-Stackoverflow-FE7A16?style=for-the-badge&logo=stack-overflow&logoColor=white)](https://stackoverflow.com/search?q=endatabas)
[![Twitter](https://img.shields.io/badge/Twitter-%231DA1F2.svg?style=for-the-badge&logo=Twitter&logoColor=white)](https://twitter.com/endatabas)
[![Mastodon](https://img.shields.io/badge/-MASTODON-%232B90D9?style=for-the-badge&logo=mastodon&logoColor=white)](https://mastodon.social/@endatabas)
[![YouTube](https://img.shields.io/badge/YouTube-%23FF0000.svg?style=for-the-badge&logo=YouTube&logoColor=white)](https://www.youtube.com/@endatabas)

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](LICENSE)

Endatabas is a SQL document database with full history.

For more information, please contact us at [hello@endatabas.com](mailto:hello@endatabas.com).

## Status

Endatabas is **experimental software.**
Learn more by reading our [roadmap](ROADMAP.md).

## Installation

* [Quickstart](https://docs.endatabas.com/tutorial/quickstart.html)
* [Installation](https://docs.endatabas.com/reference/installation.html) (additional methods)

## Documentation

* [https://docs.endatabas.com/](https://docs.endatabas.com/) contains a tutorial, reference docs, and SQL language reference.

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

Install https://www.rust-lang.org/

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
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

Install https://podman.io/ or [Docker](https://docs.docker.com/desktop/install/mac-install/):

```
sudo apt install podman podman-docker
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
