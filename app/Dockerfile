# syntax-docker/dockerfile:1

FROM golang:1.23-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY src ./src/
RUN go build -o app ./src
CMD ["/app/app"]
