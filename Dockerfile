FROM golang:1.13-alpine

LABEL maintainer="vivino.com"

ENV GOPATH /go
ENV CGO_ENABLED 0
ENV GO111MODULE on
ENV BASEPACKAGE github.com/Vivino/rankdb
ENV PACKAGEPATH /go/src/${BASEPACKAGE}/
ENV GOPROXY https://proxy.golang.org

WORKDIR ${PACKAGEPATH}

RUN  \
     apk add --no-cache git && \
     go get ${BASEPACKAGE} && \
     go build -v -o=/go/bin/rankdb ${BASEPACKAGE}/cmd/rankdb && \
     go build -v -o=/go/bin/rankdb-cli ${BASEPACKAGE}/api/tool/rankdb-cli

FROM alpine:3.10

EXPOSE 8080

COPY --from=0 /go/bin/rankdb /usr/bin/rankdb
COPY --from=0 /go/bin/rankdb-cli /usr/bin/rankdb-cli
COPY api/public /api/public
COPY api/swagger /api/swagger
COPY conf/conf.stub.toml /conf/conf.toml
COPY cmd/docker-entrypoint.sh /usr/bin/

VOLUME ["/data"]
VOLUME ["/conf"]
VOLUME ["/jwtkeys"]

HEALTHCHECK --interval=1m CMD rankdb-cli --timeout=1s health health

CMD ["rankdb"]
WORKDIR /
