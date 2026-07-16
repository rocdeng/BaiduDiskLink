FROM golang:1.25.11-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/baidudisklink ./cmd/baidudisklink

FROM alpine:3.20

RUN apk add --no-cache ca-certificates fuse3 bash tzdata
WORKDIR /app
COPY --from=build /out/baidudisklink /usr/local/bin/baidudisklink

ENV BAIDUDISKLINK_MOUNT_PATH=/mnt/baidu \
    BAIDUDISKLINK_TOKEN_PATH=/data/token.json \
    BAIDUDISKLINK_META_DB_PATH=/data/meta.db \
    BAIDUDISKLINK_OAUTH_LISTEN_ADDR=0.0.0.0:8765

VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/baidudisklink"]
