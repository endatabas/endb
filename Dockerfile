ARG RUST_OS=bullseye
ARG SBCL_OS=debian
ARG ENDB_OS=debian
ARG ENDB_GIT_REVISION="<unknown revision>"

FROM docker.io/rust:$RUST_OS AS rust-build-env

ARG RUST_OS

RUN if [ "$RUST_OS" = "alpine" ]; then \
      apk add --no-cache musl-dev; \
    fi;

WORKDIR /root/endb
COPY ./lib /root/endb

ENV RUSTFLAGS="-C target-feature=-crt-static"

RUN cargo test; cargo build --release

FROM docker.io/fukamachi/sbcl:latest-$SBCL_OS AS sbcl-build-env

ARG SBCL_OS
ARG ENDB_GIT_REVISION

RUN if [ "$SBCL_OS" = "alpine" ]; then \
      apk add --no-cache gcc musl-dev sqlite-libs; \
    else \
      apt-get update && apt-get install -y --no-install-recommends build-essential libsqlite3-0; \
    fi;

WORKDIR /root/.roswell/local-projects/endb
COPY . /root/.roswell/local-projects/endb

COPY --from=rust-build-env /root/endb/target/release/libendb.so /root/.roswell/local-projects/endb/target/libendb.so

RUN make -e ENDB_GIT_REVISION="$ENDB_GIT_REVISION" test slt-test target/endb -e CARGO=echo

FROM docker.io/$ENDB_OS

ARG ENDB_OS

RUN if [ "$ENDB_OS" = "alpine" ]; then \
      apk add --no-cache libgcc zstd-dev; \
    fi;

WORKDIR /app
COPY --from=rust-build-env /root/endb/target/release/libendb.so /app
COPY --from=sbcl-build-env /root/.roswell/local-projects/endb/target/endb /app

EXPOSE 3803
VOLUME /app/endb_data

ENTRYPOINT ["./endb"]
