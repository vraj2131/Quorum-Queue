FROM golang:1.26-alpine AS builder

WORKDIR /app
COPY go.work go.work.sum ./
COPY shared ./shared
COPY worker ./worker
COPY scheduler ./scheduler

WORKDIR /app/scheduler
RUN go build -o /app/scheduler-bin .

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/scheduler-bin /app/scheduler-bin
CMD ["/app/scheduler-bin"]
