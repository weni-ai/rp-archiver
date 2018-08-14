FROM golang:1.10-alpine

# Prepare app source directory
ENV APP_PATH /go/src/github.com/nyaruka/rp-archiver
WORKDIR $APP_PATH
COPY . .

# Install rp-archiver application
RUN go get -d -v ./... && go install -v ./cmd/...

ENTRYPOINT ["rp-archiver"]