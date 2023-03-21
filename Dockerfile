FROM ubuntu AS build-env

RUN apt-get update && apt-get install -y build-essential sbcl cl-quicklisp libsqlite3-0 curl
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

WORKDIR /root/quicklisp/local-projects/endb
COPY . /root/quicklisp/local-projects/endb

RUN sbcl --load /usr/share/common-lisp/source/quicklisp/quicklisp.lisp \
    --eval '(quicklisp-quickstart:install)' \
    --quit
RUN echo '#-quicklisp (load #P"/root/quicklisp/setup.lisp")' > /root/.sbclrc
RUN cd lib; cargo update
RUN make clean test slt-test target/endb

FROM ubuntu

WORKDIR /app
COPY --from=build-env /root/quicklisp/local-projects/endb/target/endb /app
COPY --from=build-env /root/quicklisp/local-projects/endb/target/libendb.so /app

CMD ["./endb"]
