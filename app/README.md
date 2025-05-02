# API

API for serving aggregated incident counts.


## Getting Started

To run the server locally, first build the `docker compose` stack and start
services:
```bash
$ docker compose build
$ docker compose up aggregates-db cache --wait
```

Then, run database migrations:
```bash
$ docker compose run --rm aggregates-db-migrations -e DATABASE_URL up
```

Then, run the service:
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
$ gofmt -w -s .
$ go vet ./...
```

To run tests, first set up a test database:
```bash
$ docker compose run --rm aggregates-db-migrations -e TEST_DATABASE_URL up
```

then run tests:
```bash
$ set -a && source ../.env
$ go test ./...
```

The latest database migration may be rolled back using:
```bash
$ docker compose run --rm aggregates-db-migrations -e DATABASE_URL down
```
