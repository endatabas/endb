ARG OS=ubuntu

FROM docker.io/fukamachi/sbcl:latest-$OS AS build-env

ARG OS

RUN if [ "$OS" = "alpine" ]; then \
      apk add --no-cache gcc musl-dev sqlite-libs curl bash; \
    else \
      apt-get update && apt-get install -y --no-install-recommends build-essential libsqlite3-0 curl; \
    fi;
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
ENV RUSTFLAGS="-C target-feature=-crt-static"

WORKDIR /root/.roswell/local-projects/endb
COPY . /root/.roswell/local-projects/endb

RUN cd lib; cargo update
RUN make clean test slt-test target/endb

FROM $OS

ARG OS

RUN if [ "$OS" = "alpine" ]; then \
      apk add --no-cache zstd-dev; \
    fi;

WORKDIR /app
COPY --from=build-env /root/.roswell/local-projects/endb/target/endb /app
COPY --from=build-env /root/.roswell/local-projects/endb/target/libendb.so /app

CMD ["./endb"]
