# Consumers

Consumers read acquired data from Kafka and either compute aggregates from them
or persist the acquired data directly.
 

## Getting Started

To run a consumer locally, first build the `docker compose` stack and start
dependent services:
```bash
$ docker compose build
$ docker compose up app broker warehouse --wait
```

Then, start a consumer. The type of consumer, aggregate or raw data persistence,
is determined by the `CONSUMER_TYPE` environment variable, which are set within
the [`docker compose` file](../compose.yaml). To start the e.g. raw data
persistence consumer:
```bash
$ docker compose up raw-consumer --wait
```


## Development

To run linting/formating:
```bash
$ gofmt -w -s .
$ go vet ./...
```

To run tests:
```bash
$ go test ./...
```
