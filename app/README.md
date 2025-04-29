# API

API for serving aggregated incident counts.


## Getting Started

To run the server locally, first build the `docker compose` stack from the
project's root directory and start dependent services:
```bash
$ cd ..
$ docker compose build
$ docker compose up aggregates-db --wait
```

Then, run the server:
```bash
$ docker compose up app --wait
```

and use e.g. `curl` to send a request:
```bash
$ curl -X GET "localhost:8080/aggregates?start_time=2024-01-01T00:00Z"
```


## Development

To run linting/formating:
```bash
gofmt -w -s .
go vet ./...
```

To run tests:
```bash
go test ./...
```
