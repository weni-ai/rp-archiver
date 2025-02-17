FROM golang:1.23-alpine3.20 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download -x

COPY . ./

RUN go build -o /bin/rp-archiver ./cmd/rp-archiver/*.go

FROM alpine:3.20

RUN apk add --no-cache tzdata

WORKDIR /app

COPY --from=builder bin/rp-archiver ./

EXPOSE 8080
ENTRYPOINT ["./rp-archiver"]
