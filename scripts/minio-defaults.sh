#!/usr/bin/env bash

# Docker Hub no longer serves the historical minio/minio image used by the
# local gates. Keep every MinIO launcher on the same frozen, multi-arch mirror
# of the official image while preserving per-run overrides.
DRIVE9_DEFAULT_MINIO_IMAGE="rancher/mirrored-minio-minio:RELEASE.2023-07-07T07-13-57Z"
