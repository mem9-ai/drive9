FROM public.ecr.aws/docker/library/alpine:3.19
RUN apk add --no-cache ca-certificates

COPY ./bin/drive9-server /drive9-server

EXPOSE 9009

ENTRYPOINT ["/drive9-server"]
