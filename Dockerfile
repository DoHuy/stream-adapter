FROM golang:1.11.2-alpine3.8 AS builder

ARG GITHUB_ACCESS_TOKEN
ADD . /go/src/github.com/veritone/webstream-adapter

RUN apk update && \
    apk add -U build-base git curl ffmpeg && \
    cd /go/src/github.com/veritone/webstream-adapter && \
    git config --global url."https://${GITHUB_ACCESS_TOKEN}:x-oauth-basic@github.com/".insteadOf "https://github.com/" && \
    go env && go list all | grep cover && \
    GOPATH=/go make

FROM jrottenberg/ffmpeg:4.1-alpine

ARG CONFIG_FILE=./config.json
ARG DEFAULT_PAYLOAD_FILE=./sample/payload-video.json

RUN apk update && \
    apk add -U ca-certificates gcc musl-dev python3 --no-cache && \
    pip3 install streamlink && \ 
	apk del gcc musl-dev --no-cache && \ 
	rm -rf /tmp/* &&  \
    rm -rf /var/lib/apt/lists

RUN mkdir /app
WORKDIR /app

COPY --from=builder /go/src/github.com/veritone/webstream-adapter/webstream-adapter ./webstream-adapter
ADD ${DEFAULT_PAYLOAD_FILE} payload.json
ADD ${CONFIG_FILE} config.json
ADD ./manifest.json /var/manifest.json

ENV CONFIG_FILE="/app/config.json"

ENTRYPOINT ["/app/webstream-adapter"]