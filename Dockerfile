# syntax=docker/dockerfile:1.7
FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/searxng-gateway ./cmd/gateway

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 1000 app
COPY --from=builder /out/searxng-gateway /usr/local/bin/searxng-gateway
USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/searxng-gateway"]
