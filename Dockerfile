# Build stage: static binary, CGO off (modernc.org/sqlite is pure Go).
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/scheduler ./cmd/scheduler

# Runtime stage: minimal image, non-root user.
FROM alpine:3.20
RUN adduser -D -H scheduler
COPY --from=build /out/scheduler /usr/local/bin/scheduler
USER scheduler
EXPOSE 8081
VOLUME /data
ENV SCHEDULER_DB_PATH=/data/scheduler.db \
    SCHEDULER_LISTEN=0.0.0.0:8081 \
    SCHEDULER_LOG_LEVEL=info
ENTRYPOINT ["scheduler"]
