FROM ubuntu AS build-env

RUN apt-get update && apt-get install -y build-essential sbcl cl-quicklisp cargo libsqlite3-0

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
