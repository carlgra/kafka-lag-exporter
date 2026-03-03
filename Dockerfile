FROM golang:1.22-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w -X main.version=${VERSION}" -o kafka-lag-exporter ./cmd/kafka-lag-exporter

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata && adduser -S -u 1001 kafkalagexporter
WORKDIR /opt/kafka-lag-exporter
COPY --from=builder /build/kafka-lag-exporter .
COPY config.yaml /etc/kafka-lag-exporter/config.yaml
USER 1001
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://localhost:8000/health || exit 1
ENTRYPOINT ["/opt/kafka-lag-exporter/kafka-lag-exporter"]
CMD ["--config", "/etc/kafka-lag-exporter/config.yaml"]
