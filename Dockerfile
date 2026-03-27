FROM public.ecr.aws/docker/library/alpine:3.19
RUN apk add --no-cache ca-certificates

COPY ./bin/dat9-server /dat9-server

EXPOSE 9009

ENTRYPOINT ["/dat9-server"]
