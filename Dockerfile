# Development
FROM golang:1.13.10-alpine AS development
WORKDIR /go/src/github.com/tidepool-org/marketo-service
RUN apk --no-cache update && \
    apk --no-cache upgrade && \
    apk add --no-cache ca-certificates && \
    adduser -D tidepool && \
    chown -R tidepool /go/src/github.com/tidepool-org/marketo-service
USER tidepool
COPY --chown=tidepool . .
RUN ./build.sh
CMD ["./dist/marketo-service"]