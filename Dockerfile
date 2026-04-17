FROM gcr.io/pingcap-public/pingcap/alpine:3.15.5
RUN apk add --no-cache ca-certificates

COPY ./bin/drive9-server /drive9-server

EXPOSE 9009

ENTRYPOINT ["/drive9-server"]
