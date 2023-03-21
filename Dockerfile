ARG RUST_OS=bullseye
ARG SBCL_OS=debian
ARG ENDB_OS=debian

FROM docker.io/rust:$RUST_OS AS rust-build-env

ARG RUST_OS

RUN if [ "$RUST_OS" = "alpine" ]; then \
      apk add --no-cache musl-dev; \
    fi;

WORKDIR /root/endb
COPY ./lib /root/endb

ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
ENV RUSTFLAGS="-C target-feature=-crt-static"

RUN cargo test; cargo build --release

FROM docker.io/fukamachi/sbcl:latest-$SBCL_OS AS sbcl-build-env

ARG SBCL_OS

RUN if [ "$SBCL_OS" = "alpine" ]; then \
      apk add --no-cache gcc musl-dev sqlite-libs; \
    else \
      apt-get update && apt-get install -y --no-install-recommends build-essential libsqlite3-0; \
    fi;

WORKDIR /root/.roswell/local-projects/endb
COPY . /root/.roswell/local-projects/endb

COPY --from=rust-build-env /root/endb/target/release/libendb.so /root/.roswell/local-projects/endb/target/libendb.so

RUN make test slt-test target/endb -e CARGO=echo

FROM $ENDB_OS

ARG ENDB_OS

RUN if [ "$ENDB_OS" = "alpine" ]; then \
      apk add --no-cache zstd-dev; \
    fi;

WORKDIR /app
COPY --from=rust-build-env /root/endb/target/release/libendb.so /app
COPY --from=sbcl-build-env /root/.roswell/local-projects/endb/target/endb /app

CMD ["./endb"]
