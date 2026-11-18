FROM public.ecr.aws/docker/library/alpine:3.19
RUN apk add --no-cache ca-certificates ffmpeg \
    && addgroup -g 65532 -S drive9 \
    && adduser -u 65532 -S -G drive9 -h /data drive9 \
    && mkdir -p /data \
    && chown -R drive9:drive9 /data

COPY ./bin/drive9-server /drive9-server

# Run as an unprivileged user: the server only needs its local object-store and
# scratch directories, so a server-side file read or container-level compromise
# must not double as root on the host.
USER 65532:65532

ENV DRIVE9_S3_DIR=/data/s3 \
    HOME=/data

EXPOSE 9009

ENTRYPOINT ["/drive9-server"]
