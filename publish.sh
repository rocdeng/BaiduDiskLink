#!/bin/sh
set -eu

REGISTRY=${REGISTRY:-192.168.1.5:35000}
IMAGE=${IMAGE:-baidudisklink}
TAG=$(git rev-parse --short HEAD)

docker buildx build \
  --platform linux/amd64 \
  --tag "$REGISTRY/$IMAGE:${TAG}" \
  --tag "$REGISTRY/$IMAGE:latest" \
  --push \
  .
