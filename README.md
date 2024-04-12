![Endatabas](https://www.endatabas.com/resources/images/github-banner-logo_3200x476.png)

[![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)](https://hub.docker.com/r/endatabas/endb)
[![Twitter](https://img.shields.io/badge/Twitter-%231DA1F2.svg?style=for-the-badge&logo=Twitter&logoColor=white)](https://twitter.com/endatabas)
[![Mastodon](https://img.shields.io/badge/-MASTODON-%232B90D9?style=for-the-badge&logo=mastodon&logoColor=white)](https://mastodon.social/@endatabas)
[![YouTube](https://img.shields.io/badge/YouTube-%23FF0000.svg?style=for-the-badge&logo=YouTube&logoColor=white)](https://www.youtube.com/@endatabas)

[![NPM Version](https://img.shields.io/npm/v/@endatabas/endb)](https://www.npmjs.com/package/@endatabas/endb)
[![PyPI Version](https://img.shields.io/pypi/v/endb)](https://pypi.org/project/endb/)
[![Build](https://github.com/endatabas/endb/actions/workflows/ci.yml/badge.svg)](https://github.com/endatabas/endb/actions/workflows/ci.yml)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](LICENSE)

Endatabas is a SQL document database with full history.

For more information, please contact us at [hello@endatabas.com](mailto:hello@endatabas.com).

## Status

Endatabas is in Beta and is **experimental software.**
Learn more by reading our [roadmap](ROADMAP.md).

## Installation

* [Quickstart](https://docs.endatabas.com/tutorial/quickstart.html)
* [Installation](https://docs.endatabas.com/reference/installation.html) (additional methods)

## Documentation

* [https://docs.endatabas.com/](https://docs.endatabas.com/) contains a tutorial, reference docs, and SQL language reference.
* [ARCHITECTURE.md](ARCHITECTURE.md) contains a high-level overview of the implementation.

## Building

Install https://www.sbcl.org/

On Ubuntu:

```bash
sudo apt install sbcl
```

On MacOS:

```bash
brew install sbcl
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
sudo apt install build-essential
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

Make sure to initialise ASDF source registry in the current REPL via:

```lisp
(load "_build/setup.lisp")
```

Note, this will disable all other source registries in the current Lisp process.
All Common Lisp dependencies are stored as submodules under `_build`. This directory is normally ignored by ASDF.

To ensure the submodules are up-to-date one can for example use `git pull --recurse-submodules` or:

```bash
make update-submodules
```


See https://lispcookbook.github.io/cl-cookbook/getting-started.html for details about Common Lisp development in general.

## Copyright and License

Copyright 2023-2024 Håkan Råberg and Steven Deobald.

Licensed under the GNU Affero General Public License v3.0.
