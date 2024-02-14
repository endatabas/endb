ARG RUST_OS=latest
ARG SBCL_OS=debian
ARG ENDB_OS=debian
ARG ENDB_GIT_DESCRIBE="<unknown revision>"

FROM docker.io/rust:$RUST_OS AS rust-build-env

ARG RUST_OS
ARG ENDB_GIT_DESCRIBE

RUN if [ "$RUST_OS" = "alpine" ]; then \
      apk add --no-cache musl-dev; \
    fi;

WORKDIR /root/endb
COPY ./lib /root/endb

ENV RUSTFLAGS="-C target-feature=-crt-static"
ENV ENDB_GIT_DESCRIBE=$ENDB_GIT_DESCRIBE

RUN cargo test; cargo build --release

FROM docker.io/fukamachi/sbcl:latest-$SBCL_OS AS sbcl-build-env

ARG SBCL_OS

RUN if [ "$SBCL_OS" = "alpine" ]; then \
      apk add --no-cache gcc musl-dev; \
    else \
      apt-get update && apt-get install -y --no-install-recommends build-essential; \
    fi;

WORKDIR /root/endb
COPY . /root/endb

COPY --from=rust-build-env /root/endb/target/release/libendb.so /root/endb/target/libendb.so

RUN make -e CARGO=echo test slt-test-ci target/endb

FROM docker.io/$ENDB_OS

ARG ENDB_OS

RUN if [ "$ENDB_OS" = "alpine" ]; then \
      apk add --no-cache libgcc zstd-dev; \
    fi;

WORKDIR /app
COPY --from=rust-build-env /root/endb/target/release/libendb.so /app
COPY --from=sbcl-build-env /root/endb/target/endb /app

EXPOSE 3803

ENTRYPOINT ["./endb"]
